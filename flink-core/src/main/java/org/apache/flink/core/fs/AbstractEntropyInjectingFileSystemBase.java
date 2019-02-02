/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.core.fs;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.concurrent.ThreadLocalRandom;


/**
 * Base class for entropy injecting FileSystems.
 */
public abstract class AbstractEntropyInjectingFileSystemBase extends FileSystem implements
	EntropyInjectingFileSystem {

	/**
	 * The substring to be replaced by random entropy in checkpoint paths.
	 */
	public static final ConfigOption<String> ENTROPY_INJECT_KEY_OPTION = ConfigOptions
		.key("s3.entropy.key")
		.noDefaultValue()
		.withDescription(
			"This option can be used to improve performance due to sharding issues on Amazon S3. " +
				"For file creations with entropy injection, this key will be replaced by random " +
				"alphanumeric characters. For other file creations, the key will be filtered out.");

	/**
	 * The number of entropy characters, in case entropy injection is configured.
	 */
	public static final ConfigOption<Integer> ENTROPY_INJECT_LENGTH_OPTION = ConfigOptions
		.key("s3.entropy.length")
		.defaultValue(4)
		.withDescription(
			"When '" + ENTROPY_INJECT_KEY_OPTION.key()
				+ "' is set, this option defines the number of " +
				"random characters to replace the entropy key with.");

	// ------------------------------------------------------------------------

	private static final String INVALID_ENTROPY_KEY_CHARS = "^.*[~#@*+%{}<>\\[\\]|\"\\\\].*$";

	private static final Logger LOG = LoggerFactory
		.getLogger(AbstractEntropyInjectingFileSystemBase.class);

	@Nullable
	private final String entropyInjectionKey;

	private int entropyLength;

	protected AbstractEntropyInjectingFileSystemBase(Configuration flinkConfig) {

		if (flinkConfig == null) {
			flinkConfig = new Configuration();
			LOG.warn("Creating entropy injecting filesystem with no configuration.  Will use defaults.");
		}

		// load the entropy injection settings
		this.entropyInjectionKey = flinkConfig.getString(ENTROPY_INJECT_KEY_OPTION);
		this.entropyLength = -1;
		if (entropyInjectionKey != null) {
			if (entropyInjectionKey.matches(INVALID_ENTROPY_KEY_CHARS)) {
				throw new IllegalConfigurationException("Invalid character in value for " +
					ENTROPY_INJECT_KEY_OPTION.key() + " : " + entropyInjectionKey);
			}
			this.entropyLength = flinkConfig.getInteger(ENTROPY_INJECT_LENGTH_OPTION);
			if (this.entropyLength <= 0) {
				throw new IllegalConfigurationException(
					ENTROPY_INJECT_LENGTH_OPTION.key() + " must configure a value > 0");
			}
		}
	}

	//--------------------------------------------------
	// Entropy Injection
	//--------------------------------------------------

	@Nullable
	@Override
	public String getEntropyInjectionKey() {
		return entropyInjectionKey;
	}

	@Override
	public String generateEntropy() {
		return StringUtils
			.generateRandomAlphanumericString(ThreadLocalRandom.current(), entropyLength);
	}

}
