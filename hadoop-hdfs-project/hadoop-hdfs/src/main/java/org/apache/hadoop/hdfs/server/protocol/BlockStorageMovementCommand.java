/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockSourceTargetNodePair;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;

public class BlockStorageMovementCommand extends DatanodeCommand {
  private String poolId;
  private ExtendedBlock[] blocks;
  private DatanodeInfo sourceNodes[][];
  private StorageType sourceStorageTypes[][];
  private DatanodeInfo targetNodes[][];
  private StorageType targetStorageTypes[][];
  private final long trackID;
  List<BlockInfoToMoveStorage> blockStorageMovementTasks;

  BlockStorageMovementCommand(int action, String poolId, ExtendedBlock[] blocks,
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
    this.blocks = new ExtendedBlock[pendingBlockStorageMovementsList.size()];
    this.sourceNodes = new DatanodeInfo[pendingBlockStorageMovementsList
        .size()][];
    this.sourceStorageTypes = new StorageType[pendingBlockStorageMovementsList
        .size()][];
    this.targetNodes = new DatanodeInfo[pendingBlockStorageMovementsList
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
    this.blockStorageMovementTasks = pendingBlockStorageMovementsList;
  }

  public BlockStorageMovementCommand(int action, long trackID,
      List<BlockInfoToMoveStorage> pendingBlockStorageMovementsList) {
    super(action);
    this.trackID = trackID;
    this.blockStorageMovementTasks = pendingBlockStorageMovementsList;
  }

  public List<BlockInfoToMoveStorage> getBlockStorageMovementTasks() {
    return blockStorageMovementTasks;
  }

  public long getTrackID() {
    return this.trackID;
  }

  public static class BlockInfoToMoveStorageBatch {
    private long trackId;
    public List<BlockInfoToMoveStorage> blockInfosToMoveStorages = new ArrayList<>();

    public long getTrackID() {
      return this.trackId;
    }

    public BlockInfoToMoveStorageBatch(
        List<BlockInfoToMoveStorage> blockInfoToMoveStorageBtach,
        long trackId) {
      this.trackId = trackId;
      this.blockInfosToMoveStorages.addAll(blockInfoToMoveStorageBtach);
    }

    public void addBlocksToMoveStorageBatch(
        List<BlockInfoToMoveStorage> blockInfoToMoveStorageBtach) {
      this.blockInfosToMoveStorages.addAll(blockInfoToMoveStorageBtach);
    }

  }

  public static class BlockInfoToMoveStorage {
    private ExtendedBlock blk;
    public DatanodeInfo sourceNodes[];
    public StorageType sourceStorageTypes[];
    public DatanodeInfo targetNodes[];
    public StorageType targetStorageTypes[];

    public BlockInfoToMoveStorage(ExtendedBlock block,
        DatanodeInfo[] sourceDnInfos, DatanodeInfo[] targetDnInfos,
        StorageType[] srcStorageTypes, StorageType[] targetStorageTypes) {
      this.blk = block;
      this.sourceNodes = sourceDnInfos;
      this.targetNodes = targetDnInfos;
      this.sourceStorageTypes = srcStorageTypes;
      this.targetStorageTypes = targetStorageTypes;
    }

    public BlockInfoToMoveStorage() {
    }

    public void addBlock(ExtendedBlock block) {
      this.blk = block;
    }

    public ExtendedBlock getBlock() {
      return this.blk;
    }

    public DatanodeInfo[] getSources() {
      return sourceNodes;
    }

    public DatanodeInfo[] getTargets() {
      return targetNodes;
    }

    public StorageType[] getTargetStorageTypes() {
      return targetStorageTypes;
    }

    public StorageType[] getSourceStorageTypes() {
      return sourceStorageTypes;
    }

    public void addBlocksToMoveStorage(
        List<BlockSourceTargetNodePair> blockSourceTargetNodePairs) {
      sourceNodes = new DatanodeInfo[blockSourceTargetNodePairs.size()];
      sourceStorageTypes = new StorageType[blockSourceTargetNodePairs.size()];
      targetNodes = new DatanodeInfo[blockSourceTargetNodePairs.size()];
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

    @Override
    public String toString() {
      return new StringBuilder().append("BlockInfoToMoveStorage(\n  ")
          .append("Moving block: ").append(blk).append(" From: ")
          .append(Arrays.asList(sourceNodes)).append(" To: [")
          .append(Arrays.asList(targetNodes)).append(")\n")
          .append(" sourceStorageTypes: ")
          .append(Arrays.toString(sourceStorageTypes))
          .append(" targetStorageTypes: ")
          .append(Arrays.toString(targetStorageTypes)).toString();
    }
  }

}
