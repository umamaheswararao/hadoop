package org.apache.hadoop.hdfs.server.namenode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.balancer.Matcher;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockCollection;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockSourceTargetNodePair;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.util.Daemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

public class StoragePolicySatisfier implements Runnable {
  public static final Logger LOG = LoggerFactory
      .getLogger(StoragePolicySatisfier.class);
  private Daemon storagePolicySatisfierThread = null;

  private Namesystem namesystem;
  private Configuration conf;

  private UnSatisfiedStoragePolicyFiles unSatisfiedStoragePolicyFiles;
  private StorageMovementAttemptedItems storageMovementAttemptedItems;

  private BlockManager blockManager;

  @VisibleForTesting
  public List<BlockInfoToMoveStorage> storageMismatchedBlocks;

  public StoragePolicySatisfier(final Namesystem namesystem,
      final Configuration conf, BlockManager blkManager) {
    this.namesystem = namesystem;
    this.blockManager = blkManager;
    this.conf = conf;
    unSatisfiedStoragePolicyFiles = new UnSatisfiedStoragePolicyFiles();
    this.storageMovementAttemptedItems = new StorageMovementAttemptedItems(10,
        unSatisfiedStoragePolicyFiles);
  }

  public void start() {
    storagePolicySatisfierThread = new Daemon(this);
    storagePolicySatisfierThread.setName("StoragePolicySatisfier");
    storagePolicySatisfierThread.start();
    this.storageMovementAttemptedItems.start();
  }

  public void stop() throws InterruptedException {
    this.storageMovementAttemptedItems.stop();
    storagePolicySatisfierThread.interrupt();
    storagePolicySatisfierThread.join(3000);
  }

  @Override
  public void run() {
    while (namesystem.isRunning()) {
      try {
        Long id = unSatisfiedStoragePolicyFiles.get();
        List<BlockInfoToMoveStorage> storageMismatchedBlocks = getStorageMismatchedBlocks(
            id);
        if (storageMismatchedBlocks != null) {
          distributeBlockStorageMovementTasks(id, storageMismatchedBlocks);
        }
        // Adding as attempted for movement
        storageMovementAttemptedItems.add(id);
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

  private void distributeBlockStorageMovementTasks(long trackID,
      List<BlockInfoToMoveStorage> storageMismatchedBlocks) {
    this.storageMismatchedBlocks = storageMismatchedBlocks;// TODO: this is just
                                                           // for test
    if (storageMismatchedBlocks.size() < 1) {
      return;// TODO: Major: handle this case. I think we need rerty num case to
             // be integrated.
      // Idea is, if some files are not getting storagemovement chances, then we
      // can just retry limited number of times and exit.
    }
    DatanodeDescriptor coordinatorNode = storageMismatchedBlocks
        .get(0).sourceNodes[0];
    coordinatorNode.addBlocksToMoveStorage(
        new BlockInfoToMoveStorageBatch(storageMismatchedBlocks));
  }

  private List<BlockInfoToMoveStorage> getStorageMismatchedBlocks(
      long inodeID) {
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
    List<BlockInfoToMoveStorage> blockInfoToMoveStorages = new ArrayList<BlockInfoToMoveStorage>();
    for (int i = 0; i < blocks.length; i++) {
      BlockInfo blockInfo = blocks[i];
      List<StorageType> newTypes = existingStoragePolicy
          .chooseStorageTypes(blockInfo.getReplication());
      DatanodeStorageInfo[] storages = blockManager.getStorages(blockInfo);
      StorageType storageTypes[] = new StorageType[storages.length];
      for (int j = 0; j < storages.length; j++) {
        DatanodeStorageInfo datanodeStorageInfo = storages[j];
        StorageType storageType = datanodeStorageInfo.getStorageType();
        storageTypes[j] = storageType;
      }
      final StorageTypeDiff diff = new StorageTypeDiff(newTypes, storageTypes);
      if (!diff.removeOverlap(true)) {
        List<StorageTypeNodePair> sourceWithStorageMap = new ArrayList<StorageTypeNodePair>();
        List<DatanodeStorageInfo> existingBlockStorages = new ArrayList<DatanodeStorageInfo>(
            Arrays.asList(storages));
        for (StorageType existingType : diff.existing) {
          Iterator<DatanodeStorageInfo> iterator = existingBlockStorages
              .iterator();
          while (iterator.hasNext()) {
            DatanodeStorageInfo datanodeStorageInfo = iterator.next();
            StorageType storageType = datanodeStorageInfo.getStorageType();
            if (storageType == existingType) {
              iterator.remove();
              sourceWithStorageMap.add(new StorageTypeNodePair(storageType,
                  datanodeStorageInfo.getDatanodeDescriptor()));
              break;
            }
          }
        }

        BlockInfoToMoveStorage blkInfoToMoveStorage = new BlockInfoToMoveStorage();
        blkInfoToMoveStorage.addBlock(blockInfo);
        StorageTypeNodeMap locsForExpectedStorageTypes = getTargetLocsForExpectedStorageTypes(
            diff.expected);

        List<BlockSourceTargetNodePair> blockSourceTargetNodePairs = buildSourceAndTaregtMapping(
            blockInfo, diff.existing, sourceWithStorageMap, diff.expected,
            locsForExpectedStorageTypes);
        blkInfoToMoveStorage.addBlocksToMoveStorage(blockSourceTargetNodePairs);

        blockInfoToMoveStorages.add(blkInfoToMoveStorage);
      }
    }
    return blockInfoToMoveStorages;
  }

  /**
   * Find the good target node for each source node which was misplaced in wrong
   * storage.
   * 
   * @param blockInfo
   * @param existing
   * @param sourceWithStorageList
   * @param expected
   * @param locsForExpectedStorageTypes
   * @return
   */
  private List<BlockSourceTargetNodePair> buildSourceAndTaregtMapping(
      BlockInfo blockInfo, List<StorageType> existing,
      List<StorageTypeNodePair> sourceWithStorageList,
      List<StorageType> expected,
      StorageTypeNodeMap locsForExpectedStorageTypes) {
    List<BlockSourceTargetNodePair> sourceTargetNodePairList = new ArrayList<>();
    List<DatanodeDescriptor> chosenNodes = new ArrayList<>();
    for (StorageTypeNodePair existingTypeNodePair : sourceWithStorageList) {
      StorageTypeNodePair chosenTarget = chooseTargetTypeInSameNode(
          existingTypeNodePair.dn, expected, locsForExpectedStorageTypes,
          chosenNodes);

      if (chosenTarget == null && blockManager.getDatanodeManager()
          .getNetworkTopology().isNodeGroupAware()) {
        chosenTarget = chooseTarget(blockInfo, existingTypeNodePair.dn,
            expected, Matcher.SAME_NODE_GROUP, locsForExpectedStorageTypes,
            chosenNodes);
      }

      // Then, match nodes on the same rack
      if (chosenTarget == null) {
        chosenTarget = chooseTarget(blockInfo, existingTypeNodePair.dn,
            expected, Matcher.SAME_RACK, locsForExpectedStorageTypes,
            chosenNodes);
      }

      if (chosenTarget == null) {
        chosenTarget = chooseTarget(blockInfo, existingTypeNodePair.dn,
            expected, Matcher.ANY_OTHER, locsForExpectedStorageTypes,
            chosenNodes);
      }
      if (null != chosenTarget) {
        sourceTargetNodePairList.add(new BlockSourceTargetNodePair(
            existingTypeNodePair.dn, existingTypeNodePair.storageType,
            chosenTarget.dn, chosenTarget.storageType));
        chosenNodes.add(chosenTarget.dn);
        // TODO: check wether this is right place
        chosenTarget.dn.incrementBlocksScheduled(chosenTarget.storageType);
      } else {
        // TODO: Failed to ChooseTargetNodes...So let just retry. Shall we
        // proceed without this targets? Then what should be final result?
        // How about pack emty target, means target node could not be chosen ,
        // so result should be RETRY_REQUIRED from DN always.
        // Log..unable to choose target node for source datanodeDescriptor
        sourceTargetNodePairList
            .add(new BlockSourceTargetNodePair(existingTypeNodePair.dn,
                existingTypeNodePair.storageType, null, null));
      }
    }

    return sourceTargetNodePairList;

  }

  /**
   * Choose the target storage within same Datanode if possible.
   * 
   * @param locsForExpectedStorageTypes
   * @param chosenNodes
   */
  StorageTypeNodePair chooseTargetTypeInSameNode(DatanodeDescriptor source,
      List<StorageType> targetTypes,
      StorageTypeNodeMap locsForExpectedStorageTypes,
      List<DatanodeDescriptor> chosenNodes) {
    for (StorageType t : targetTypes) {
      DatanodeStorageInfo chooseStorage4Block = source.chooseStorage4Block(t,
          0);
      if (chooseStorage4Block != null) {
        return new StorageTypeNodePair(t, source);
      }
    }
    return null;
  }

  StorageTypeNodePair chooseTarget(Block block, DatanodeDescriptor source,
      List<StorageType> targetTypes, Matcher matcher,
      StorageTypeNodeMap locsForExpectedStorageTypes,
      List<DatanodeDescriptor> chosenNodes) {
    for (StorageType t : targetTypes) {
      List<DatanodeDescriptor> nodesWithStorages = locsForExpectedStorageTypes
          .getNodesWithStorages(t);
      Collections.shuffle(nodesWithStorages);
      for (DatanodeDescriptor target : nodesWithStorages) {
        if (!chosenNodes.contains(target) && matcher.match(
            blockManager.getDatanodeManager().getNetworkTopology(), source,
            target)) {
          if (null != target.chooseStorage4Block(t, block.getNumBytes())) {
            return new StorageTypeNodePair(t, target);
          }
        }
      }
    }
    return null;
  }

  class StorageTypeNodePair {
    public StorageType storageType = null;
    public DatanodeDescriptor dn = null;

    public StorageTypeNodePair(StorageType storageType, DatanodeDescriptor dn) {
      this.storageType = storageType;
      this.dn = dn;
    }
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
  // TODO: move this class to util package and use it here and in mover as this
  // one is copied from there for now.
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

  // TODO: similar data structures are there in Mover. make it refined later.
  static class StorageTypeNodeMap {
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

    List<DatanodeDescriptor> getNodesWithStorages(StorageType type) {
      return nodeStorageTypeMap.get(type);
    }
  }

  public class BlockInfoToMoveStorage {
    private Block blk;
    public DatanodeDescriptor sourceNodes[];
    public StorageType sourceStorageTypes[];
    public DatanodeDescriptor targetNodes[];
    public StorageType targetStorageTypes[];

    public void addBlock(Block block) {
      this.blk = block;
    }

    public Block getBlock() {
      return this.blk;
    }

    public void addBlocksToMoveStorage(
        List<BlockSourceTargetNodePair> blockSourceTargetNodePairs) {
      sourceNodes = new DatanodeDescriptor[blockSourceTargetNodePairs.size()];
      sourceStorageTypes = new StorageType[blockSourceTargetNodePairs.size()];
      targetNodes = new DatanodeDescriptor[blockSourceTargetNodePairs.size()];
      targetStorageTypes = new StorageType[blockSourceTargetNodePairs.size()];

      for (int i = 0; i < blockSourceTargetNodePairs.size(); i++) {
        sourceNodes[i] = blockSourceTargetNodePairs.get(i).sourceNode;
        sourceStorageTypes[i] = blockSourceTargetNodePairs
            .get(i).sourceStorageType;
        targetNodes[i] = blockSourceTargetNodePairs.get(i).targetNode;
        targetStorageTypes[i] = blockSourceTargetNodePairs
            .get(i).targetStorageType;
      }
    }

  }

  public class BlockInfoToMoveStorageBatch {
    private long trackId;
    public List<BlockInfoToMoveStorage> blockInfosToMoveStorages = new ArrayList<>();

    public long getTrackID() {
      return this.trackId;
    }

    public BlockInfoToMoveStorageBatch(
        List<BlockInfoToMoveStorage> blockInfoToMoveStorageBtach) {

      this.blockInfosToMoveStorages.addAll(blockInfoToMoveStorageBtach);
    }

    public void addBlocksToMoveStorageBatch(
        List<BlockInfoToMoveStorage> blockInfoToMoveStorageBtach) {
      this.blockInfosToMoveStorages.addAll(blockInfoToMoveStorageBtach);
    }

  }

  /**
   * 
   * @param inodeID
   */
  public void add(long inodeID) {
    unSatisfiedStoragePolicyFiles.add(inodeID);
  }

}
