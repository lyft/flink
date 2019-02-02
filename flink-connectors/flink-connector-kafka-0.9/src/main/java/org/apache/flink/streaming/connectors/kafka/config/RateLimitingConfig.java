/*
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

package org.apache.flink.streaming.connectors.kafka.config;

import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import java.util.Properties;

/**
 * A generic RateLimitingConfig that can be used to set ratelimiting properties across
 * consumers.
 */
public class RateLimitingConfig {

	/** Flag that indicates if ratelimiting is enabled. */
	private static final String RATELIMITING_FLAG = "consumer.ratelimiting.enabled";

	/** Max bytes per second that can be read by the consumer. */
	private static final String RATELIMITING_MAX_BYTES_PER_SECOND = "consumer.ratelimiting.maxbytespersecond";

	/** Delimiter for properties. */
	private static final String DELIMITER = ".";

	/** Default value for ratelimiting flag. */
	private static final boolean DEFAULT_USE_RATELIMITING = false;

	/**
	 *
	 * @param consumerPrefix consumer name.
	 * @param properties
	 * @return Get rate limiting flag.
	 */
	public static boolean getRatelimitFlag(String consumerPrefix, Properties properties) {
		return Boolean.valueOf(properties.getProperty(getRatelimitingFlagProperty(consumerPrefix),
			String.valueOf(DEFAULT_USE_RATELIMITING)));
	}

	/**
	 *
	 * @param consumerPrefix consumer name.
	 * @param properties
	 * @return Get max bytes per second.
	 */
	public static long getMaxBytesPerSecond(String consumerPrefix, Properties properties) {
		return Long.valueOf(properties.getProperty(getMaxBytesPerSecondProperty(consumerPrefix)));
	}

	/**
	 *
	 * @param consumerPrefix consumer name.
	 * @param properties
	 * @param flag
	 *
	 */
	public static void setRateLimitFlag(String consumerPrefix, Properties properties, boolean flag) {
		properties.setProperty(getRatelimitingFlagProperty(consumerPrefix), String.valueOf(flag));
	}

	/**
	 *
	 * @param consumerPrefix consumer name.
	 * @param properties
	 * @param maxBytes
	 *
	 */
	public static void setGlobalMaxBytesPerSecond(String consumerPrefix, Properties properties, long maxBytes) {
		properties.setProperty(getMaxBytesPerSecondProperty(consumerPrefix), String.valueOf(maxBytes));
	}

	/**
	 * If ratelimiting is enabled, set maxBytesPerSecond per consumer based on a global ratelimit.
	 * @param runtimeContext
	 * @param properties
	 * @param consumerPrefix Consumer name.
	 */
	public static void setLocalMaxBytesPerSecond(
		StreamingRuntimeContext runtimeContext,
		Properties properties,
		String consumerPrefix) {
		boolean useRatelimiting = getRatelimitFlag(consumerPrefix, properties);

		if (useRatelimiting) {
			long globalRatelimit = getMaxBytesPerSecond(consumerPrefix, properties);
			//Calculate maxBytesPersecond per consumer
			long localRatelimit = globalRatelimit / runtimeContext.getNumberOfParallelSubtasks();
			String maxBytesProperty = getMaxBytesPerSecondProperty(consumerPrefix);
			properties.setProperty(maxBytesProperty, String.valueOf(localRatelimit));
		}
	}

	private static String getRatelimitingFlagProperty(String consumerPrefix) {
		return consumerPrefix.concat(DELIMITER).concat(RATELIMITING_FLAG);
	}

	private static String getMaxBytesPerSecondProperty(String consumerPrefix) {
		return consumerPrefix.concat(DELIMITER).concat(RATELIMITING_MAX_BYTES_PER_SECOND);
	}

}
