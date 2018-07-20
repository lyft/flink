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

package org.apache.flink.streaming.connectors.kinesis.internals;

import org.apache.flink.streaming.connectors.kinesis.metrics.ShardMetricsReporter;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShardState;
import org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.model.SequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyInterface;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kinesis.testutils.FakeKinesisBehavioursFactory;
import org.apache.flink.streaming.connectors.kinesis.testutils.KinesisShardIdGenerator;
import org.apache.flink.streaming.connectors.kinesis.testutils.TestSourceContext;
import org.apache.flink.streaming.connectors.kinesis.testutils.TestableKinesisDataFetcher;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import com.amazonaws.services.kinesis.model.HashKeyRange;
import com.amazonaws.services.kinesis.model.Shard;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.mockito.Mockito;

import java.math.BigInteger;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@link ShardConsumer}.
 */
public class ShardConsumerTest {

	@Test
	public void testCorrectNumOfCollectedRecordsAndUpdatedState() {
		StreamShardHandle fakeToBeConsumedShard = new StreamShardHandle(
			"fakeStream",
			new Shard()
				.withShardId(KinesisShardIdGenerator.generateFromShardOrder(0))
				.withHashKeyRange(
					new HashKeyRange()
						.withStartingHashKey("0")
						.withEndingHashKey(new BigInteger(StringUtils.repeat("FF", 16), 16).toString())));

		LinkedList<KinesisStreamShardState> subscribedShardsStateUnderTest = new LinkedList<>();
		subscribedShardsStateUnderTest.add(
			new KinesisStreamShardState(KinesisDataFetcher.convertToStreamShardMetadata(fakeToBeConsumedShard),
				fakeToBeConsumedShard, new SequenceNumber("fakeStartingState")));

		TestSourceContext<String> sourceContext = new TestSourceContext<>();

		TestableKinesisDataFetcher<String> fetcher =
			new TestableKinesisDataFetcher<>(
				Collections.singletonList("fakeStream"),
				sourceContext,
				new Properties(),
				new KinesisDeserializationSchemaWrapper<>(new SimpleStringSchema()),
				10,
				2,
				new AtomicReference<>(),
				subscribedShardsStateUnderTest,
				KinesisDataFetcher.createInitialSubscribedStreamsToLastDiscoveredShardsState(Collections.singletonList("fakeStream")),
				Mockito.mock(KinesisProxyInterface.class));

		new ShardConsumer<>(
			fetcher,
			0,
			subscribedShardsStateUnderTest.get(0).getStreamShardHandle(),
			subscribedShardsStateUnderTest.get(0).getLastProcessedSequenceNum(),
			FakeKinesisBehavioursFactory.totalNumOfRecordsAfterNumOfGetRecordsCalls(1000, 9), new ShardMetricsReporter()).run();

		assertEquals(1000, sourceContext.getCollectedOutputs().size());
		assertEquals(
			SentinelSequenceNumber.SENTINEL_SHARD_ENDING_SEQUENCE_NUM.get(),
			subscribedShardsStateUnderTest.get(0).getLastProcessedSequenceNum());
	}

	@Test
	public void testCorrectNumOfCollectedRecordsAndUpdatedStateWithUnexpectedExpiredIterator() {
		StreamShardHandle fakeToBeConsumedShard = new StreamShardHandle(
			"fakeStream",
			new Shard()
				.withShardId(KinesisShardIdGenerator.generateFromShardOrder(0))
				.withHashKeyRange(
					new HashKeyRange()
						.withStartingHashKey("0")
						.withEndingHashKey(new BigInteger(StringUtils.repeat("FF", 16), 16).toString())));

		LinkedList<KinesisStreamShardState> subscribedShardsStateUnderTest = new LinkedList<>();
		subscribedShardsStateUnderTest.add(
			new KinesisStreamShardState(KinesisDataFetcher.convertToStreamShardMetadata(fakeToBeConsumedShard),
				fakeToBeConsumedShard, new SequenceNumber("fakeStartingState")));

		TestSourceContext<String> sourceContext = new TestSourceContext<>();

		TestableKinesisDataFetcher<String> fetcher =
			new TestableKinesisDataFetcher<>(
				Collections.singletonList("fakeStream"),
				sourceContext,
				new Properties(),
				new KinesisDeserializationSchemaWrapper<>(new SimpleStringSchema()),
				10,
				2,
				new AtomicReference<>(),
				subscribedShardsStateUnderTest,
				KinesisDataFetcher.createInitialSubscribedStreamsToLastDiscoveredShardsState(Collections.singletonList("fakeStream")),
				Mockito.mock(KinesisProxyInterface.class));

		new ShardConsumer<>(
			fetcher,
			0,
			subscribedShardsStateUnderTest.get(0).getStreamShardHandle(),
			subscribedShardsStateUnderTest.get(0).getLastProcessedSequenceNum(),
			// Get a total of 1000 records with 9 getRecords() calls,
			// and the 7th getRecords() call will encounter an unexpected expired shard iterator
			FakeKinesisBehavioursFactory.totalNumOfRecordsAfterNumOfGetRecordsCallsWithUnexpectedExpiredIterator(1000, 9, 7), new ShardMetricsReporter()).run();

		assertEquals(1000, sourceContext.getCollectedOutputs().size());
		assertEquals(
			SentinelSequenceNumber.SENTINEL_SHARD_ENDING_SEQUENCE_NUM.get(),
			subscribedShardsStateUnderTest.get(0).getLastProcessedSequenceNum());
	}

	@Test
	public void testCorrectNumOfCollectedRecordsAndUpdatedStateWithAdaptiveReads() {
		Properties consumerProperties = new Properties();
		consumerProperties.put("flink.shard.use.adaptive.reads", "true");
		StreamShardHandle fakeToBeConsumedShard = new StreamShardHandle(
			"fakeStream",
			new Shard()
				.withShardId(KinesisShardIdGenerator.generateFromShardOrder(0))
				.withHashKeyRange(
					new HashKeyRange()
						.withStartingHashKey("0")
						.withEndingHashKey(new BigInteger(StringUtils.repeat("FF", 16), 16).toString())));

		LinkedList<KinesisStreamShardState> subscribedShardsStateUnderTest = new LinkedList<>();
		subscribedShardsStateUnderTest.add(
			new KinesisStreamShardState(KinesisDataFetcher.convertToStreamShardMetadata(fakeToBeConsumedShard),
				fakeToBeConsumedShard, new SequenceNumber("fakeStartingState")));

		TestSourceContext<String> sourceContext = new TestSourceContext<>();

		TestableKinesisDataFetcher<String> fetcher =
			new TestableKinesisDataFetcher<>(
				Collections.singletonList("fakeStream"),
				sourceContext,
				consumerProperties,
				new KinesisDeserializationSchemaWrapper<>(new SimpleStringSchema()),
				10,
				2,
				new AtomicReference<>(),
				subscribedShardsStateUnderTest,
				KinesisDataFetcher.createInitialSubscribedStreamsToLastDiscoveredShardsState(Collections.singletonList("fakeStream")),
				Mockito.mock(KinesisProxyInterface.class));

		new ShardConsumer<>(
			fetcher,
			0,
			subscribedShardsStateUnderTest.get(0).getStreamShardHandle(),
			subscribedShardsStateUnderTest.get(0).getLastProcessedSequenceNum(),
			// Start with inital number of records per batch as 10
			// Make 2 calls to getRecords
			// Each record is of size 10 Kb
			FakeKinesisBehavioursFactory.initialNumOfRecordsAfterNumOfGetRecordsCallsWithAdaptiveReads(10, 2), new ShardMetricsReporter()).run();

		// Avg record size for first batch --> 10 * 10 Kb/10 = 10 Kb
		// Number of records fetched in second batch --> 2 Mb/10Kb * 5 = 40
		// Total number of records = 10 + 40 = 50
		assertEquals(50, sourceContext.getCollectedOutputs().size());
		assertEquals(
			SentinelSequenceNumber.SENTINEL_SHARD_ENDING_SEQUENCE_NUM.get(),
			subscribedShardsStateUnderTest.get(0).getLastProcessedSequenceNum());
	}

}
