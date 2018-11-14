package org.apache.flink.runtime.watermark;

import java.io.Serializable;

public class GlobalSourceWatermarkInfo implements Serializable {

	private static final long serialVersionUID = 1L;
	private SourceWatermark min;
	private SourceWatermark max;

	public GlobalSourceWatermarkInfo() {
		min = new SourceWatermark(Long.MIN_VALUE);
		max = new SourceWatermark(Long.MIN_VALUE);
	}

	public SourceWatermark getMin() {
		return min;
	}

	public void setMin(SourceWatermark min) {
		this.min = min;
	}

	public SourceWatermark getMax() {
		return max;
	}

	public void setMax(SourceWatermark max) {
		this.max = max;
	}
}
