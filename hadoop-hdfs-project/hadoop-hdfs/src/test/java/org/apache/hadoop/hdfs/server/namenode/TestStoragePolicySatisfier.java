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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;

public class TestStoragePolicySatisfier {
  private static final Logger LOG = LoggerFactory
      .getLogger(TestStoragePolicySatisfier.class);
  private FSNamesystem namesystem;

  @Test(timeout = 300000)
  public void testMoveWhenStoragePolicyNotSatisfying() throws Exception {
    // HDFS-8147
    final Configuration conf = new HdfsConfiguration();
    conf.setLong("dfs.block.size", 1024);
    // start 10 datanodes
    int numOfDatanodes = 3;
    int storagesPerDatanode=2;
    long capacity = 2 * 256 * 1024 * 1024;
    long[][] capacities = new long[numOfDatanodes][storagesPerDatanode];
    for (int i = 0; i < numOfDatanodes; i++) {
      for(int j=0;j<storagesPerDatanode;j++){
        capacities[i][j]=capacity;
      }
    }
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(numOfDatanodes)
        .storagesPerDatanode(storagesPerDatanode)
        .storageTypes(new StorageType[][]{
            {StorageType.DISK, StorageType.DISK},
            {StorageType.DISK, StorageType.DISK},
                { StorageType.DISK, StorageType.DISK } })
        .storageCapacities(capacities)
        .build();

    try {
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();

      final String file = "/testMoveWhenStoragePolicyNotSatisfying";
      // write to DISK
      final FSDataOutputStream out = dfs.create(new Path(file));
      for (int i = 0; i < 1000; i++) {
        out.writeChars("t");
      }
      out.close();

      // move to ARCHIVE
      dfs.setStoragePolicy(new Path(file), "COLD");

      namesystem = cluster.getNamesystem();
      INode inode = namesystem.getFSDirectory().getINode(file);



      /*
       * // set "/bar" directory with HOT storage policy. ClientProtocol client
       * = NameNodeProxies.createProxy(conf, cluster.getFileSystem(0).getUri(),
       * ClientProtocol.class).getProxy();
       */

      numOfDatanodes += 3;
      capacities = new long[3][storagesPerDatanode];
      for (int i = 0; i < 3; i++) {
        for (int j = 0; j < storagesPerDatanode; j++) {
          capacities[i][j] = capacity;
        }
      }

      cluster.startDataNodes(conf, 3,
          new StorageType[][] { { StorageType.ARCHIVE, StorageType.ARCHIVE },
              { StorageType.ARCHIVE, StorageType.ARCHIVE },
              { StorageType.ARCHIVE, StorageType.ARCHIVE },
              { StorageType.ARCHIVE, StorageType.ARCHIVE },
              { StorageType.ARCHIVE, StorageType.ARCHIVE } },
          true, null, null, null, capacities, null, false, false, false, null);
      cluster.triggerHeartbeats();

      namesystem.getBlockManager().satisfyStoragePolicy(inode.getId());

      cluster.triggerHeartbeats();

      // Wait till namenode notified about the block location details
      waitForLocatedBlockWithArchiveStorageType(dfs, file, 3, 30000);
    } finally {
      cluster.shutdown();
    }
  }

  private void waitForLocatedBlockWithArchiveStorageType(
      final DistributedFileSystem dfs, final String file,
      int expectedArchiveCount, int timeout) throws Exception {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        LocatedBlock lb = null;
        try {
          lb = dfs.getClient().getLocatedBlocks(file, 0).get(0);
        } catch (IOException e) {
          LOG.error("Exception while getting located blocks", e);
          return false;
        }
        int archiveCount = 0;
        for (StorageType storageType : lb.getStorageTypes()) {
          if (StorageType.ARCHIVE == storageType) {
            archiveCount++;
          }
        }
        LOG.info("Archive replica count, expected={} and actual={}",
            expectedArchiveCount, archiveCount);
        return expectedArchiveCount == archiveCount;
      }
    }, 100, timeout);
  }
}
