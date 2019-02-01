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

	/**
	 *
	 * @param consumerPrefix consumer name.
	 * @return fully defined property with rate limiting flag.
	 */
	public static String getRatelimitFlag(String consumerPrefix) {
		return consumerPrefix.concat(DELIMITER).concat(RATELIMITING_FLAG);
	}

	/**
	 *
	 * @param consumerPrefix consumer name.
	 * @return a fully defined property with max bytes per second.
	 */
	public static String getRatelimitMaxBytesPerSecond(String consumerPrefix) {
		return consumerPrefix.concat(DELIMITER).concat(RATELIMITING_MAX_BYTES_PER_SECOND);
	}

}
