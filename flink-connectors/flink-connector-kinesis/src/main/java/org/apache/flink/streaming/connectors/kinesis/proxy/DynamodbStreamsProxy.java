/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kinesis.proxy;

import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.ClientConfigurationFactory;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.LimitExceededException;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.StreamStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants.AWS_ENDPOINT;
import static org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants.AWS_REGION;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.DEFAULT_DYNAMODB_STREAM_DESCRIBE_BACKOFF_BASE;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.DEFAULT_DYNAMODB_STREAM_DESCRIBE_BACKOFF_EXPONENTIAL_CONSTANT;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.DEFAULT_DYNAMODB_STREAM_DESCRIBE_BACKOFF_MAX;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.DYNAMODB_STREAM_DESCRIBE_BACKOFF_BASE;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.DYNAMODB_STREAM_DESCRIBE_BACKOFF_EXPONENTIAL_CONSTANT;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.DYNAMODB_STREAM_DESCRIBE_BACKOFF_MAX;
import static org.apache.flink.streaming.connectors.kinesis.util.AWSUtil.getCredentialsProvider;
import static org.apache.flink.streaming.connectors.kinesis.util.AWSUtil.setAwsClientConfigProperties;

/**
 * DynamoDB streams proxy: interface interacting with the DynamoDB streams.
 */
public class DynamodbStreamsProxy extends KinesisProxy {
	private static final Logger LOG = LoggerFactory.getLogger(DynamodbStreamsProxy.class);

	/** Used for formatting Flink-specific user agent string when creating Kinesis client. */
	private static final String USER_AGENT_FORMAT = "Apache Flink %s (%s) DynamoDB Streams Connector";

	// Backoff millis for the describe stream operation.
	private final long describeStreamBaseBackoffMillis;
	// Maximum backoff millis for the describe stream operation.
	private final long describeStreamMaxBackoffMillis;
	// Exponential backoff power constant for the describe stream operation.
	private final double describeStreamExpConstant;

	protected DynamodbStreamsProxy(Properties configProps) {
		super(configProps);

		// parse properties
		describeStreamBaseBackoffMillis = Long.valueOf(
				configProps.getProperty(DYNAMODB_STREAM_DESCRIBE_BACKOFF_BASE,
						Long.toString(DEFAULT_DYNAMODB_STREAM_DESCRIBE_BACKOFF_BASE)));
		describeStreamMaxBackoffMillis = Long.valueOf(
				configProps.getProperty(DYNAMODB_STREAM_DESCRIBE_BACKOFF_MAX,
						Long.toString(DEFAULT_DYNAMODB_STREAM_DESCRIBE_BACKOFF_MAX)));
		describeStreamExpConstant = Double.valueOf(
				configProps.getProperty(DYNAMODB_STREAM_DESCRIBE_BACKOFF_EXPONENTIAL_CONSTANT,
						Double.toString(DEFAULT_DYNAMODB_STREAM_DESCRIBE_BACKOFF_EXPONENTIAL_CONSTANT)));
	}


	/**
	 * Creates a DynamoDB streams proxy.
	 *
	 * @param configProps configuration properties
	 * @return the created DynamoDB streams proxy
	 */
	public static KinesisProxyInterface create(Properties configProps) {
		return new DynamodbStreamsProxy(configProps);
	}

	/**
	 * Creates an AmazonDynamoDBStreamsAdapterClient.
	 * Uses it as the internal client interacting with the DynamoDB streams.
	 *
	 * @param configProps configuration properties
	 * @return an AWS DynamoDB streams adapter client
	 */
	@Override
	protected AmazonKinesis createKinesisClient(Properties configProps) {
		ClientConfiguration awsClientConfig = new ClientConfigurationFactory().getConfig();
		setAwsClientConfigProperties(awsClientConfig, configProps);

		AWSCredentialsProvider credentials = getCredentialsProvider(configProps);
		awsClientConfig.setUserAgentPrefix(
				String.format(
						USER_AGENT_FORMAT,
						EnvironmentInformation.getVersion(),
						EnvironmentInformation.getRevisionInformation().commitId));

		AmazonDynamoDBStreamsAdapterClient adapterClient =
				new AmazonDynamoDBStreamsAdapterClient(credentials, awsClientConfig);

		if (configProps.containsKey(AWS_ENDPOINT)) {
			adapterClient.setEndpoint(configProps.getProperty(AWS_ENDPOINT));
		} else {
			adapterClient.setRegion(Region.getRegion(
					Regions.fromName(configProps.getProperty(AWS_REGION))));
		}

		return adapterClient;
	}

	@Override
	public GetShardListResult getShardList(
			Map<String, String> streamNamesWithLastSeenShardIds) throws InterruptedException {
		GetShardListResult result = new GetShardListResult();

		for (Map.Entry<String, String> streamNameWithLastSeenShardId :
				streamNamesWithLastSeenShardIds.entrySet()) {
			String stream = streamNameWithLastSeenShardId.getKey();
			String lastSeenShardId = streamNameWithLastSeenShardId.getValue();
			result.addRetrievedShardsToStream(stream, getShardsOfStream(stream, lastSeenShardId));
		}
		return result;
	}

	private List<StreamShardHandle> getShardsOfStream(
			String streamName,
			@Nullable String lastSeenShardId)
			throws InterruptedException {
		List<StreamShardHandle> shardsOfStream = new ArrayList<>();

		DescribeStreamResult describeStreamResult;
		do {
			describeStreamResult = describeStream(streamName, lastSeenShardId);
			List<Shard> shards = describeStreamResult.getStreamDescription().getShards();
			for (Shard shard : shards) {
				shardsOfStream.add(new StreamShardHandle(streamName, shard));
			}

			if (shards.size() != 0) {
				lastSeenShardId = shards.get(shards.size() - 1).getShardId();
			}
		} while (describeStreamResult.getStreamDescription().isHasMoreShards());

		return shardsOfStream;
	}

	/**
	 * Get metainfo for a Dynamodb stream, which contains information about which shards this
	 * Dynamodb stream possess.
	 *
	 * <p>This method is using a "full jitter" approach described in AWS's article,
	 * <a href="https://www.awsarchitectureblog.com/2015/03/backoff.html">
	 *   "Exponential Backoff and Jitter"</a>.
	 * This is necessary because concurrent calls will be made by all parallel subtask's fetcher.
	 * This jitter backoff approach will help distribute calls across the fetchers over time.
	 *
	 * @param streamName the stream to describe
	 * @param startShardId which shard to start with for this describe operation
	 *
	 * @return the result of the describe stream operation
	 */
	private DescribeStreamResult describeStream(String streamName, @Nullable String startShardId)
			throws InterruptedException {
		final DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
		describeStreamRequest.setStreamName(streamName);
		describeStreamRequest.setExclusiveStartShardId(startShardId);

		DescribeStreamResult describeStreamResult = null;

		// Call DescribeStream, with full-jitter backoff (if we get LimitExceededException).
		int attemptCount = 0;
		while (describeStreamResult == null) { // retry until we get a result
			try {
				describeStreamResult = kinesisClient.describeStream(describeStreamRequest);
			} catch (LimitExceededException le) {
				long backoffMillis = fullJitterBackoff(
						describeStreamBaseBackoffMillis,
						describeStreamMaxBackoffMillis,
						describeStreamExpConstant,
						attemptCount++);
				LOG.warn(String.format("Got LimitExceededException when describing stream %s. "
						+ "Backing off for %d millis.", streamName, backoffMillis));
				Thread.sleep(backoffMillis);
			} catch (ResourceNotFoundException re) {
				throw new RuntimeException("Error while getting stream details", re);
			}
		}

		String streamStatus = describeStreamResult.getStreamDescription().getStreamStatus();
		if (!(streamStatus.equals(StreamStatus.ACTIVE.toString())
				|| streamStatus.equals(StreamStatus.UPDATING.toString()))) {
			if (LOG.isWarnEnabled()) {
				LOG.warn(String.format("The status of stream %s is %s ; result of the current "
								+ "describeStream operation will not contain any shard information.",
						streamName, streamStatus));
			}
		}

		return describeStreamResult;
	}

}
