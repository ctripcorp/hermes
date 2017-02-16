package com.ctrip.hermes.kafka.server;

import kafka.api.TopicMetadata;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MockKafkaClusterTest {

	private MockZookeeper zk;

	private MockKafkaCluster cluster;

	@Before
	public void before() {
		zk = new MockZookeeper();
		cluster = new MockKafkaCluster(zk, 3);
	}

	@After
	public void after() {
		cluster.stop();
		zk.stop();
	}

	@Test
	public void testConnectionStr() {
		Assert.assertEquals("localhost:9092,localhost:9093,localhost:9094", cluster.getConnectionString());
	}

	@Test
	public void createTopicWithMultiplePartitions() {
		String topic = "mytopic";
		cluster.createTopic(topic, 3, 2);
		TopicMetadata fetchTopicMetadata = cluster.fetchTopicMetadata(topic);
		Assert.assertEquals(3, fetchTopicMetadata.partitionsMetadata().size());
	}
}
