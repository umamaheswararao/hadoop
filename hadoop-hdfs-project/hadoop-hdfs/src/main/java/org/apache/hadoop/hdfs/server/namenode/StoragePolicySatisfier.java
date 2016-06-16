package org.apache.hadoop.hdfs.server.namenode;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockCollection;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

public class StoragePolicySatisfier implements Runnable {
  public static final Logger LOG = LoggerFactory
      .getLogger(StoragePolicySatisfier.class);

  private Namesystem namesystem;
  private Configuration conf;

  private UnSatisfiedStoragePolicyFiles unSatisfiedStoragePloicyFiles;

  private BlockManager blockManager;

  @VisibleForTesting
  public List<BlockInfoToMoveStorage> storageMismatchedBlocks;

  public StoragePolicySatisfier(final Namesystem namesystem,
      final Configuration conf, BlockManager blkManager,
      UnSatisfiedStoragePolicyFiles unSatisfiedStoragePloicyFiles) {
    this.namesystem = namesystem;
    this.blockManager = blkManager;
    this.conf = conf;
    this.unSatisfiedStoragePloicyFiles = unSatisfiedStoragePloicyFiles;
  }

  @Override
  public void run() {
    while (namesystem.isRunning()) {
      try {
        List<BlockInfoToMoveStorage> storageMismatchedBlocks = getStorageMismatchedBlocks();
        satisfyBlockStorageLocations(storageMismatchedBlocks);
        Thread.sleep(3000);
      } catch (Throwable t) {
        if (!namesystem.isRunning()) {
          LOG.info("Stopping StoragePolicySatisfier.");
          if (!(t instanceof InterruptedException)) {
            LOG.info("StoragePolicySatisfier received an exception"
                + " while shutting down.", t);
          }
          break;
        }
        LOG.error("StoragePolicySatisfier thread received runtime exception. ",
            t);
        // terminate(1, t);
      }
    }
  }

  private void satisfyBlockStorageLocations(
      List<BlockInfoToMoveStorage> storageMismatchedBlocks) {
    this.storageMismatchedBlocks = storageMismatchedBlocks;
    // Find source node and assign as commands
  }

  private List<BlockInfoToMoveStorage> getStorageMismatchedBlocks() {
    Long inodeID = unSatisfiedStoragePloicyFiles.get();
    BlockCollection blockCollection = namesystem.getBlockCollection(inodeID);
    if (blockCollection == null) {
      return null;
    }
    byte existingStoragePolicyID = blockCollection.getStoragePolicyID();
    BlockStoragePolicy existingStoragePolicy = blockManager
        .getStoragePolicy(existingStoragePolicyID);
    if (!blockCollection.getLastBlock().isComplete()) {
      // Postpone, currently file is under construction
      // So, should we add back? or leave it to user
      return null;
    }

    BlockInfo[] blocks = blockCollection.getBlocks();
    BlockInfoToMoveStorage[] blockInfoToMoveStorage = new BlockInfoToMoveStorage[blocks.length];
    for (int i = 0; i < blocks.length; i++) {
      BlockInfo blockInfo = blocks[i];
      List<StorageType> newTypes = existingStoragePolicy
          .chooseStorageTypes((short) blocks.length);
      DatanodeStorageInfo[] storages = blockManager.getStorages(blockInfo);
      StorageType storageTypes[] = new StorageType[storages.length];
      StorageTypeNodeMap sourceWithStorageMap = new StorageTypeNodeMap();
      for (int j = 0; j < storages.length; j++) {
        DatanodeStorageInfo datanodeStorageInfo = storages[j];
        StorageType storageType = datanodeStorageInfo.getStorageType();
        storageTypes[j] = storageType;
        sourceWithStorageMap.add(storageType,
            datanodeStorageInfo.getDatanodeDescriptor());
      }
      final StorageTypeDiff diff = new StorageTypeDiff(newTypes, storageTypes);
      if (!diff.removeOverlap(true)) {
        blockInfoToMoveStorage[i] = new BlockInfoToMoveStorage();
        blockInfoToMoveStorage[i].addBlock(blockInfo);
        // blockInfoToMoveStorage[i].setExpectedStorageTypes(diff.expected);
        // blockInfoToMoveStorage[i].setExistingStorageTypes(diff.existing);
        StorageTypeNodeMap locsForExpectedStorageTypes = getTargetLocsForExpectedStorageTypes(
            diff.expected);
        blockInfoToMoveStorage[i]
            .setTargetsLocsForExpectedStorages(locsForExpectedStorageTypes);
        blockInfoToMoveStorage[i]
            .setSourceLocForExistingStorages(sourceWithStorageMap);
      }
    }
    return Arrays.asList(blockInfoToMoveStorage);
  }

  private StorageTypeNodeMap getTargetLocsForExpectedStorageTypes(
      List<StorageType> expected) {
    StorageTypeNodeMap targetMap = new StorageTypeNodeMap();
    List<DatanodeDescriptor> reports = blockManager.getDatanodeManager()
        .getDatanodeListForReport(DatanodeReportType.LIVE);
    for (DatanodeDescriptor dn : reports) {
      StorageReport[] storageReports = dn.getStorageReports();
      for (StorageReport storageReport : storageReports) {
        StorageType t = storageReport.getStorage().getStorageType();
        if (expected.contains(t)) {
          final long maxRemaining = getMaxRemaining(dn.getStorageReports(), t);
          if (maxRemaining > 0L) {
            targetMap.add(t, dn);
          }
        }
      }
    }
    return targetMap;
  }

  private static long getMaxRemaining(StorageReport[] storageReports,
      StorageType t) {
    long max = 0L;
    for (StorageReport r : storageReports) {
      if (r.getStorage().getStorageType() == t) {
        if (r.getRemaining() > max) {
          max = r.getRemaining();
        }
      }
    }
    return max;
  }

  @VisibleForTesting
  // NOTE: copied from Mover.
  static class StorageTypeDiff {
    final List<StorageType> expected;
    final List<StorageType> existing;

    StorageTypeDiff(List<StorageType> expected, StorageType[] existing) {
      this.expected = new LinkedList<StorageType>(expected);
      this.existing = new LinkedList<StorageType>(Arrays.asList(existing));
    }

    /**
     * Remove the overlap between the expected types and the existing types.
     *
     * @param ignoreNonMovable
     *          ignore non-movable storage types by removing them from both
     *          expected and existing storage type list to prevent non-movable
     *          storage from being moved.
     * @returns if the existing types or the expected types is empty after
     *          removing the overlap.
     */
    boolean removeOverlap(boolean ignoreNonMovable) {
      for (Iterator<StorageType> i = existing.iterator(); i.hasNext();) {
        final StorageType t = i.next();
        if (expected.remove(t)) {
          i.remove();
        }
      }
      if (ignoreNonMovable) {
        removeNonMovable(existing);
        removeNonMovable(expected);
      }
      return expected.isEmpty() || existing.isEmpty();
    }

    void removeNonMovable(List<StorageType> types) {
      for (Iterator<StorageType> i = types.iterator(); i.hasNext();) {
        final StorageType t = i.next();
        if (!t.isMovable()) {
          i.remove();
        }
      }
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "{expected=" + expected
          + ", existing=" + existing + "}";
    }
  }

  private static class StorageTypeNodeMap {
    private final EnumMap<StorageType, List<DatanodeDescriptor>> nodeStorageTypeMap = new EnumMap<StorageType, List<DatanodeDescriptor>>(
        StorageType.class);

    private void add(StorageType t, DatanodeDescriptor dn) {
      List<DatanodeDescriptor> nodesWithStorages = getNodesWithStorages(t);
      LinkedList<DatanodeDescriptor> value = null;
      if (nodesWithStorages == null) {
        value = new LinkedList<DatanodeDescriptor>();
        value.add(dn);
        nodeStorageTypeMap.put(t, value);
      } else {
        nodesWithStorages.add(dn);
      }
    }

    private List<DatanodeDescriptor> getNodesWithStorages(StorageType type) {
      return nodeStorageTypeMap.get(type);
    }
  }

  public class BlockInfoToMoveStorage {

    private Block blk;
    private StorageTypeNodeMap locsForExpectedStorageTypes;
    private StorageTypeNodeMap sourceWithStorageMap;

    public void addBlock(Block block) {
      this.blk = block;
    }

    public void setSourceLocForExistingStorages(
        StorageTypeNodeMap sourceWithStorageMap) {
      this.sourceWithStorageMap = sourceWithStorageMap;
    }

    public StorageTypeNodeMap getSourceLocForExistingStorages() {
      return this.sourceWithStorageMap;
    }

    public Block getBlock() {
      return this.blk;
    }

    public void setTargetsLocsForExpectedStorages(
        StorageTypeNodeMap locsForExpectedStorageTypes) {
      this.locsForExpectedStorageTypes = locsForExpectedStorageTypes;

    }

    public StorageTypeNodeMap getTargetsLocsForExpectedStorages(
        StorageTypeNodeMap locsForExpectedStorageTypes) {
      return this.locsForExpectedStorageTypes;

    }
  }
}
