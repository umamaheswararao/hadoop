package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Test;

public class TestStoragePolicySatisfier {
  private FSNamesystem namesystem;

  @Test(timeout = 300000000)
  public void testMoveWhenStoragePolicyNotSatisfying() throws Exception {
    // HDFS-8147
    final Configuration conf = new HdfsConfiguration();
    // start 10 datanodes
    int numOfDatanodes = 3;
    int storagesPerDatanode=2;
    long capacity = 256 * 1024 * 1024;
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
      out.writeChars("testMoveWhenStoragePolicyNotSatisfying");
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

      Thread.sleep(1000000000);
      System.out
          .println(namesystem.getBlockManager().sps.storageMismatchedBlocks);

    } finally {
      cluster.shutdown();
    }
  }
}
