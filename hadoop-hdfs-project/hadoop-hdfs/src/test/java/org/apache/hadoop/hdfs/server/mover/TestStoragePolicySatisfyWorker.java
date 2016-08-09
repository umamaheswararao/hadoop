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
package org.apache.hadoop.hdfs.server.mover;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.balancer.TestBalancer;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.StoragePolicySatisfyWorker;
import org.apache.hadoop.hdfs.server.protocol.StoragePolicySatisfyMovementCommand.BlockToMoveStoragePair;
import org.junit.Assert;
import org.junit.Test;

/**
 * This class tests the behavior of moving block replica to the given storage
 * type to fulfill the storage policy requirement.
 *
 */
public class TestStoragePolicySatisfyWorker {

  static final int DEFAULT_BLOCK_SIZE = 100;
  File keytabFile;
  String principal;

  static {
    TestBalancer.initTestSetup();
  }

  static void initConf(Configuration conf) {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BLOCK_SIZE);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 1L);
    conf.setLong(DFSConfigKeys.DFS_BALANCER_MOVEDWINWIDTH_KEY, 2000L);
  }

  @Test(timeout = 300000)
  public void testMoveSingleBlockToAnotherDatanode() throws Exception {
    // HDFS-8147
    final Configuration conf = new HdfsConfiguration();
    initConf(conf);
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(4)
        .storageTypes(
            new StorageType[][] { { StorageType.DISK, StorageType.ARCHIVE },
                { StorageType.DISK, StorageType.ARCHIVE },
                { StorageType.DISK, StorageType.ARCHIVE },
                { StorageType.DISK, StorageType.ARCHIVE } })
        .build();
    try {
      cluster.waitActive();
      DataNodeProperties stopDataNode = cluster.stopDataNode(3);
      final DistributedFileSystem dfs = cluster.getFileSystem();
      final String file = "/testMoveSingleBlockToAnotherDatanode";
      // write to DISK
      final FSDataOutputStream out = dfs.create(new Path(file), (short) 2);
      out.writeChars("testMoveSingleBlockToAnotherDatanode");
      out.close();

      // verify before movement
      LocatedBlock lb = dfs.getClient().getLocatedBlocks(file, 0).get(0);
      StorageType[] storageTypes = lb.getStorageTypes();
      for (StorageType storageType : storageTypes) {
        Assert.assertTrue(StorageType.DISK == storageType);
      }
      // move to ARCHIVE
      dfs.setStoragePolicy(new Path(file), "COLD");

      lb = dfs.getClient().getLocatedBlocks(file, 0).get(0);
      cluster.restartDataNode(stopDataNode, true);
      cluster.waitActive();
      DataNode src = cluster.getDataNodes().get(3);
      DatanodeInfo targetDnInfo = DFSTestUtil
          .getLocalDatanodeInfo(src.getXferPort());

      StoragePolicySatisfyWorker worker = new StoragePolicySatisfyWorker(conf,
          src);
      Collection<BlockToMoveStoragePair> blockToMoveStorageTasks = new ArrayList<>();
      BlockToMoveStoragePair storagePair = prepareBlockToMoveStoragePair(
          lb.getBlock(), lb.getLocations()[0], targetDnInfo,
          lb.getStorageTypes()[0], StorageType.ARCHIVE);
      blockToMoveStorageTasks.add(storagePair);
      worker.processBlockToMoveStorageTasks(blockToMoveStorageTasks);
      cluster.triggerHeartbeats();

      // Wait till namenode notified
      Thread.sleep(3000);
      lb = dfs.getClient().getLocatedBlocks(file, 0).get(0);
      storageTypes = lb.getStorageTypes();
      int archiveCount = 0;
      for (StorageType storageType : storageTypes) {
        if (StorageType.ARCHIVE == storageType) {
          archiveCount++;
        }
      }
      Assert.assertEquals(archiveCount, 1);
    } finally {
      cluster.shutdown();
    }
  }

  BlockToMoveStoragePair prepareBlockToMoveStoragePair(ExtendedBlock block,
      DatanodeInfo src, DatanodeInfo destin, StorageType storageType,
      StorageType targetStorageType) {
    return new BlockToMoveStoragePair(block, new DatanodeInfo[] { src },
        new DatanodeInfo[] { destin }, new StorageType[] { storageType },
        new StorageType[] { targetStorageType });
  }

}
