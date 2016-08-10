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

import static org.apache.hadoop.util.Time.monotonicNow;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hdfs.server.protocol.StorageMovementResult;
import org.apache.hadoop.util.Daemon;

public class StorageMovementAttemptedItems {
  private final Map<Long, Long> storageMovementAttemptedItems;
  private final Map<Long, StorageMovementResult> storageMovementAttemptedResults;
  private volatile boolean fsRunning = true;
  Daemon timerThread = null;
  //
  // It might take anywhere between 5 to 10 minutes before
  // a request is timed out.
  //
  private long timeout = 5 * 60 * 1000;
  private UnSatisfiedStoragePolicyFiles unsatisfiedStorageMovementFiles;
  private final static long DEFAULT_RECHECK_INTERVAL = 5 * 60 * 1000;


  public StorageMovementAttemptedItems(long timeoutPeriod,
      UnSatisfiedStoragePolicyFiles unsatisfiedStorageMovementFiles) {
    if (timeoutPeriod > 0) {
      this.timeout = timeoutPeriod;
    }
    this.unsatisfiedStorageMovementFiles = unsatisfiedStorageMovementFiles;
    storageMovementAttemptedItems = new HashMap<>();
    storageMovementAttemptedResults = new HashMap<>();
  }

  public void add(Long inodeID) {
    storageMovementAttemptedItems.put(inodeID, monotonicNow());
  }

  public void addResult(Long inodeID, StorageMovementResult result) {
    storageMovementAttemptedResults.put(inodeID, result);
  }

  void start() {
    timerThread = new Daemon(new StorageMovementAttemptResultMonitor());
    timerThread.start();
  }
  public void stop() {
    fsRunning = false;
  }

  class StorageMovementAttemptResultMonitor implements Runnable {
    @Override
    public void run() {
      while (fsRunning) {
        long period = Math.min(DEFAULT_RECHECK_INTERVAL, timeout);
        try {
          storageMovementResultCheck();
          Thread.sleep(period);
        } catch (InterruptedException ie) {
          // LOG.debug("PendingReconstructionMonitor thread is interrupted.",
          // ie);
        }
      }
    }

    private void storageMovementResultCheck() {
      synchronized (storageMovementAttemptedItems) {
        Iterator<Entry<Long, Long>> iter = storageMovementAttemptedItems
            .entrySet().iterator();
        long now = monotonicNow();
        while (iter.hasNext()) {
          Entry<Long, Long> entry = iter.next();
          if (now > entry.getValue() + timeout) {
            Long fileInodeID = entry.getKey();
            StorageMovementResult storageMovementResult = storageMovementAttemptedResults
                .get(fileInodeID);
            if (storageMovementResult != null) {
              if (storageMovementResult
                  .getResult() == StorageMovementResult.StorageMovementTrialResult.RETRY_REQUIRED) {
                // TODO: Add back to unsatisfied list
                unsatisfiedStorageMovementFiles.add(fileInodeID);
              }
              storageMovementAttemptedResults.remove(fileInodeID);
            }
            // TODO:remove
            iter.remove();
          }
        }
      }

    }
  }

}
