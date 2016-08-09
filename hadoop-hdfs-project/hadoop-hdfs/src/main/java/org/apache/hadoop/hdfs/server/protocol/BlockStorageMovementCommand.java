package org.apache.hadoop.hdfs.server.protocol;

import java.util.List;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.namenode.StoragePolicySatisfier.BlockInfoToMoveStorage;

public class BlockStorageMovementCommand extends DatanodeCommand {
  final String poolId;
  private Block[] blocks;
  public DatanodeDescriptor sourceNodes[][];
  public StorageType sourceStorageTypes[][];
  public DatanodeDescriptor targetNodes[][];
  public StorageType targetStorageTypes[][];
  public long trackID;


  BlockStorageMovementCommand(int action, String poolId, Block[] blocks,
      long trackID,
      DatanodeDescriptor sourceNodes[][], StorageType sourceStorageTypes[][],
      DatanodeDescriptor targetNodes[][], StorageType targetStorageTypes[][]) {
    super(action);
    this.blocks = blocks;
    this.poolId = poolId;
    this.trackID = trackID;
    this.sourceNodes = sourceNodes;
    this.sourceStorageTypes = sourceStorageTypes;
    this.targetNodes = targetNodes;
    this.targetStorageTypes = targetStorageTypes;
  }

  public BlockStorageMovementCommand(int action, String poolId, long trackID,
      List<BlockInfoToMoveStorage> pendingBlockStorageMovementsList) {
    super(action);
    this.poolId = poolId;
    this.trackID = trackID;
    this.blocks = new Block[pendingBlockStorageMovementsList.size()];
    this.sourceNodes = new DatanodeDescriptor[pendingBlockStorageMovementsList
        .size()][];
    this.sourceStorageTypes = new StorageType[pendingBlockStorageMovementsList
        .size()][];
    this.targetNodes = new DatanodeDescriptor[pendingBlockStorageMovementsList
        .size()][];
    this.targetStorageTypes = new StorageType[pendingBlockStorageMovementsList
        .size()][];
    for (int i = 0; i < blocks.length; i++) {
      BlockInfoToMoveStorage p = pendingBlockStorageMovementsList.get(i);
      blocks[i] = p.getBlock();
      sourceNodes[i] = p.sourceNodes;
      sourceStorageTypes[i] = p.sourceStorageTypes;
      targetNodes[i] = p.targetNodes;
      targetStorageTypes[i] = p.targetStorageTypes;
    }
  }

}
