package org.apache.hadoop.hdfs.server.namenode;

import java.util.LinkedList;
import java.util.Queue;

public class UnSatisfiedStoragePolicyFiles {
  // TODO: need to handle concurrency
  Queue<Long> usatisfiedStoragePolicyFiles = new LinkedList<Long>();

  public void add(Long inodeID) {
    usatisfiedStoragePolicyFiles.add(inodeID);
  }

  public void remove(Long inodeID) {
    usatisfiedStoragePolicyFiles.remove(inodeID);
  }

  public long get() {
    Long nodeid = usatisfiedStoragePolicyFiles.poll();
    return nodeid == null ? 0 : nodeid;
  }
}
