package org.apache.hadoop.hdfs.server.protocol;

public class StorageMovementResult {
	static public enum StorageMovementTrialResult {
		SUCCESS, RETRY_REQUIRED, CANNOT_MOVE;
	}

	private long trackID;
	private StorageMovementTrialResult result;

	public StorageMovementResult(long trackID, StorageMovementTrialResult result) {
		this.trackID = trackID;
		this.result = result;
	}

	public long getTrackID() {
		return this.trackID;
	}

	public StorageMovementTrialResult getResult() {
		return this.result;
	}
}
