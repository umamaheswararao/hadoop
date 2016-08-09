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
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import static org.apache.hadoop.hdfs.protocolPB.PBHelperClient.vintPrefixed;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataEncryptionKeyFactory;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.DNConf;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.protocol.StoragePolicySatisfyMovementCommand.BlockToMoveStoragePair;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Daemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StoragePolicySatisfyWorker handles the storage policy satisfier commands.
 * These commands would be issued from Namenode as part of Datanode's heart beat
 * response. BPOfferService delegates the work to this class for handling SPS
 * commands.
 */
@InterfaceAudience.Private
public class StoragePolicySatisfyWorker {
  private static final Logger LOG = LoggerFactory
      .getLogger(StoragePolicySatisfyWorker.class);
  private int retryMaxAttempts; // TODO: Failure retries to be supported

  // TODO: manage concurrency based on configuration
  private final int maxConcurrentMovesPerNode;
  private final long movedWinWidth; // TODO: need to ensure confifured bandwidth

  private final int moverThreads;
  private ExecutorService moveExecutor;
  private CompletionService<Void> moverExecutorCompletionService;

  private final DataNode datanode;
  private final int ioFileBufferSize;

  public StoragePolicySatisfyWorker(Configuration conf, DataNode datanode) {
    movedWinWidth = conf.getLong(DFSConfigKeys.DFS_MOVER_MOVEDWINWIDTH_KEY,
        DFSConfigKeys.DFS_MOVER_MOVEDWINWIDTH_DEFAULT);
    moverThreads = conf.getInt(DFSConfigKeys.DFS_MOVER_MOVERTHREADS_KEY,
        DFSConfigKeys.DFS_MOVER_MOVERTHREADS_DEFAULT);
    maxConcurrentMovesPerNode = conf.getInt(
        DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY,
        DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_DEFAULT);
    this.retryMaxAttempts = conf.getInt(
        DFSConfigKeys.DFS_MOVER_RETRY_MAX_ATTEMPTS_KEY,
        DFSConfigKeys.DFS_MOVER_RETRY_MAX_ATTEMPTS_DEFAULT);
    moveExecutor = initializeBlockMoverThreadPool(moverThreads);
    moverExecutorCompletionService = new ExecutorCompletionService<>(
        moveExecutor);
    this.datanode = datanode;
    this.ioFileBufferSize = DFSUtilClient.getIoFileBufferSize(conf);
  }

  private ThreadPoolExecutor initializeBlockMoverThreadPool(int num) {
    LOG.debug("Block mover to satisfy storage policy; pool threads={}", num);

    ThreadPoolExecutor moverThreadPool = new ThreadPoolExecutor(1, num, 60,
        TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
        new Daemon.DaemonFactory() {
          private final AtomicInteger threadIndex = new AtomicInteger(0);

          @Override
          public Thread newThread(Runnable r) {
            Thread t = super.newThread(r);
            t.setName("BlockMoverTask-" + threadIndex.getAndIncrement());
            return t;
          }
        }, new ThreadPoolExecutor.CallerRunsPolicy() {
          @Override
          public void rejectedExecution(Runnable runnable,
              ThreadPoolExecutor e) {
            LOG.info(
                "Execution for block movement to satisfy storage policy got rejected, "
                    + "Executing in current thread");
            // will run in the current thread
            super.rejectedExecution(runnable, e);
          }
        });

    moverThreadPool.allowCoreThreadTimeOut(true);
    return moverThreadPool;
  }

  enum MovementStatus {
    DN_STORAGE_MOVEMENT_SUCCESS, /* */
    DN_STORAGE_MOVEMENT_FAILURE_GENSTAMP_MISMATCH, /* */
    DN_STORAGE_MOVEMENT_FAILURE_NW_ERROR, /* */
    DN_STORAGE_MOVEMENT_FAILURE_NO_SPACE, /* */
    DN_STORAGE_MOVEMENT_FAILURE_BLOCK_PINNED; /* */
  }

  public void processBlockToMoveStorageTasks(
      Collection<BlockToMoveStoragePair> blockToMoveStorageTasks) {
    List<Future<Void>> moverTaskFuturesMap = new ArrayList<>();
    Future<Void> moveCallable = null;
    for (BlockToMoveStoragePair blockToMoveStoragePair : blockToMoveStorageTasks) {
      assert blockToMoveStoragePair
          .getSources().length == blockToMoveStoragePair.getTargets().length;

      BlockToMoveStorageTask blockToMoveStorageTask = new BlockToMoveStorageTask(
          blockToMoveStoragePair.getBlock(),
          blockToMoveStoragePair.getSources(),
          blockToMoveStoragePair.getTargets(),
          blockToMoveStoragePair.getTargetStorageTypes());
      moveCallable = moverExecutorCompletionService
          .submit(blockToMoveStorageTask);
      moverTaskFuturesMap.add(moveCallable);
    }

    for (int i = 0; i < moverTaskFuturesMap.size(); i++) {
      try {
        moveCallable = moverExecutorCompletionService.take();
        moveCallable.get();
      } catch (InterruptedException | ExecutionException e) {
        // TODO: failure retries and report back the error to Namenode
        LOG.error("Exception while moving block replica to target storage type",
            e);
      }
    }
  }

  /**
   * This class encapsulates the process of moving the block replica to the
   * given target.
   */
  private class BlockToMoveStorageTask implements Callable<Void> {
    private final ExtendedBlock block;
    private final DatanodeInfo[] sources;
    private final DatanodeInfo[] targets;
    private final StorageType[] targetStorageTypes;

    public BlockToMoveStorageTask(ExtendedBlock block, DatanodeInfo[] sources,
        DatanodeInfo[] targets, StorageType[] targetStorageTypes) {
      this.block = block;
      this.sources = sources;
      this.targets = targets;
      this.targetStorageTypes = targetStorageTypes;
    }

    @Override
    public Void call() {
      moveBlock();
      return null;
    }

    private void moveBlock() {
      LOG.info("Start moving block {}", block);

      for (int i = 0; i < sources.length; i++) {
        moveBlockToSource(block, sources[i], targets[i], targetStorageTypes[i]);
      }
    }

    private void moveBlockToSource(ExtendedBlock block, DatanodeInfo srcDn,
        DatanodeInfo targetDn, StorageType targetStorageType) {
      LOG.debug("Start moving block:{} from src:{} to destin:{} to satisfy "
          + "storageType:{}", block, srcDn, targetDn, targetStorageType);
      Socket sock = null;
      DataOutputStream out = null;
      DataInputStream in = null;
      try {
        DNConf dnConf = datanode.getDnConf();
        String dnAddr = targetDn
            .getXferAddr(dnConf.getConnectToDnViaHostname());
        sock = datanode.newSocket();
        NetUtils.connect(sock, NetUtils.createSocketAddr(dnAddr),
            dnConf.getSocketTimeout());
        sock.setSoTimeout(2 * dnConf.getSocketTimeout());
        LOG.debug("Connecting to datanode {}", dnAddr);

        OutputStream unbufOut = sock.getOutputStream();
        InputStream unbufIn = sock.getInputStream();

        Token<BlockTokenIdentifier> accessToken = datanode.getBlockAccessToken(
            block, EnumSet.of(BlockTokenIdentifier.AccessMode.WRITE));

        DataEncryptionKeyFactory keyFactory = datanode
            .getDataEncryptionKeyFactoryForBlock(block);
        IOStreamPair saslStreams = datanode.getSaslClient().socketSend(sock,
            unbufOut, unbufIn, keyFactory, accessToken, targetDn);
        unbufOut = saslStreams.out;
        unbufIn = saslStreams.in;
        out = new DataOutputStream(
            new BufferedOutputStream(unbufOut, ioFileBufferSize));
        in = new DataInputStream(
            new BufferedInputStream(unbufIn, ioFileBufferSize));
        sendRequest(out, block, accessToken, srcDn, targetStorageType);
        receiveResponse(in);

        LOG.debug(
            "Successfully moved block:{} from src:{} to destin:{} for"
                + " satisfying storageType:{}",
            block, srcDn, targetDn, targetStorageType);
      } catch (IOException e) {
        // TODO: failure retries
        LOG.warn(
            "Failed to move block:{} from src:{} to destin:{} to satisfy "
                + "storageType:{}",
            block, srcDn, targetDn, targetStorageType, e);
      } finally {
        IOUtils.closeStream(out);
        IOUtils.closeStream(in);
        IOUtils.closeSocket(sock);
      }

    }

    /** Send a reportedBlock replace request to the output stream */
    private void sendRequest(DataOutputStream out, ExtendedBlock eb,
        Token<BlockTokenIdentifier> accessToken, DatanodeInfo srcDn,
        StorageType targetStorageType) throws IOException {
      new Sender(out).replaceBlock(eb, targetStorageType, accessToken,
          srcDn.getDatanodeUuid(), srcDn);
    }

    /** Receive a reportedBlock copy response from the input stream */
    private void receiveResponse(DataInputStream in) throws IOException {
      BlockOpResponseProto response = BlockOpResponseProto
          .parseFrom(vintPrefixed(in));
      while (response.getStatus() == Status.IN_PROGRESS) {
        // read intermediate responses
        response = BlockOpResponseProto.parseFrom(vintPrefixed(in));
      }
      String logInfo = "reportedBlock move is failed";
      DataTransferProtoUtil.checkBlockOpStatus(response, logInfo);
    }
  }
}
