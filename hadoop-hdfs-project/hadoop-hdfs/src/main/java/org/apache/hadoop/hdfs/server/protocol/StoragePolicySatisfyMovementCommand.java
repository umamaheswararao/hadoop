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

import java.util.Collection;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;

/**
 * A StoragePolicySatisfyMovementCommand is an instruction to the DataNode to
 * move the replicas to the given storage type in order to fulfill the storage
 * policy requirement.
 *
 * Upon receiving this command, the DataNode moves replica to target storage
 * type based on the storage pair information present in the command.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class StoragePolicySatisfyMovementCommand extends DatanodeCommand {
  private final Collection<BlockToMoveStoragePair> blockToMoveStoragePair;

  public StoragePolicySatisfyMovementCommand(int action,
      Collection<BlockToMoveStoragePair> blockToMoveStoragePair) {
    super(action);
    this.blockToMoveStoragePair = blockToMoveStoragePair;
  }

  public Collection<BlockToMoveStoragePair> getBlockToMoveStorageTasks() {
    return blockToMoveStoragePair;
  }

  /** Block and storage pair information. */
  public static class BlockToMoveStoragePair {
    private final ExtendedBlock block;
    private final DatanodeInfo[] sources;
    private final DatanodeInfo[] targets;
    private final StorageType[] targetStorageTypes;
    private final StorageType[] existingStorageTypes;

    public BlockToMoveStoragePair(ExtendedBlock block, DatanodeInfo[] sources,
        DatanodeInfo[] targets, StorageType[] existingStorageTypes,
        StorageType[] targetStorageTypes) {
      this.block = block;
      this.sources = sources;
      this.targets = targets;
      this.existingStorageTypes = existingStorageTypes;
      this.targetStorageTypes = targetStorageTypes;
    }

    public ExtendedBlock getBlock() {
      return block;
    }

    public DatanodeInfo[] getSources() {
      return sources;
    }

    public DatanodeInfo[] getTargets() {
      return targets;
    }

    public StorageType[] getTargetStorageTypes() {
      return targetStorageTypes;
    }

    public StorageType[] getExistingStorageTypes() {
      return existingStorageTypes;
    }
  }
}
