package org.apache.hadoop.hdfs.server.namenode;

import java.util.LinkedList;
import java.util.Queue;

public class UnSatisfiedStoragePolicyFiles {
  // TODO: need to handle concurrency

  //
  // It might take anywhere between 5 to 10 minutes before
  // a request is timed out.
  //
  private long timeout = 5 * 60 * 1000;
  private Queue<Long> usatisfiedStoragePolicyFiles = new LinkedList<>();
  private final StorageMovementAttemptedItems storageMovementAttemptedItems = new StorageMovementAttemptedItems(
      timeout, this);

  public synchronized void add(Long inodeID) {
    usatisfiedStoragePolicyFiles.add(inodeID);
  }

  public synchronized long get() {
    Long nodeid = usatisfiedStoragePolicyFiles.poll();
    if (nodeid == null) {
      return 0;
    }
    return nodeid;
  }
}
