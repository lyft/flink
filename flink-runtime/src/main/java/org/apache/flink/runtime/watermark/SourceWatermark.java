package org.apache.flink.runtime.watermark;

import java.io.Serializable;

public class SourceWatermark implements Serializable {

	private static final long serialVersionUID = 1L;
	private long timestamp;

	public SourceWatermark(long timestamp) {
		this.timestamp = timestamp;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
}
