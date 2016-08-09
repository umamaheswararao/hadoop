package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.fs.StorageType;

public class BlockSourceTargetNodePair {

  public DatanodeDescriptor sourceNode;
  public StorageType sourceStorageType;
  public DatanodeDescriptor targetNode;
  public StorageType targetStorageType;

  public BlockSourceTargetNodePair(DatanodeDescriptor sourceNode,
      StorageType sourceStorageType, DatanodeDescriptor targetNode,
      StorageType targetStorageType) {
    this.sourceNode = sourceNode;
    this.sourceStorageType = sourceStorageType;
    this.targetNode = targetNode;
    this.targetStorageType = targetStorageType;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder
        .append(" [Source Node : " + (sourceNode != null ? sourceNode : null));
    builder.append(", StorageType : "
        + (sourceStorageType != null ? sourceStorageType : null) + "]");
    builder.append(
        " --> [Target Node : " + (targetNode != null ? targetNode : null));
    builder.append(", StorageType : "
        + (targetStorageType != null ? targetStorageType : null) + "]");
    return builder.toString();
  }
}