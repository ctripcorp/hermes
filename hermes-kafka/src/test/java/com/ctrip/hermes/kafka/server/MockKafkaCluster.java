package com.ctrip.hermes.kafka.server;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import scala.collection.Iterator;
import scala.collection.Seq;
import kafka.api.PartitionMetadata;
import kafka.api.TopicMetadata;

public class MockKafkaCluster {

	private List<MockKafka> kafkaCluster;

	private String connectionStr;

	public MockKafkaCluster(MockZookeeper zkServer, int clusterSize) {
		kafkaCluster = new ArrayList<MockKafka>();
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < clusterSize; i++) {
			MockKafka node = new MockKafka(zkServer, String.valueOf(9092 + i), String.valueOf(i + 1));
			kafkaCluster.add(node);
			sb.append(node.getConnectionString()).append(',');
		}
		sb.deleteCharAt(sb.length() - 1);
		connectionStr = sb.toString();
	}

	public void start() {
		for (MockKafka node : kafkaCluster) {
			node.start();
		}
	}

	public void stop() {
		for (MockKafka node : kafkaCluster) {
			node.stop();
		}
	}

	public String getConnectionString() {
		return connectionStr;
	}

	public void createTopic(String topic, int partition, int replication) {
		Random random = new Random();
		MockKafka kafka = kafkaCluster.get(random.nextInt(kafkaCluster.size()));
		kafka.createTopic(topic, partition, replication);
	}

	public void deleteTopic(String topic) {
		Random random = new Random();
		MockKafka kafka = kafkaCluster.get(random.nextInt(kafkaCluster.size()));
		kafka.deleteTopic(topic);
	}

	public TopicMetadata fetchTopicMetadata(String topic) {
		Random random = new Random();
		MockKafka kafka = kafkaCluster.get(random.nextInt(kafkaCluster.size()));
		return kafka.fetchTopicMeta(topic);
	}

	public TopicMetadata waitTopicUntilReady(String topic) {
		boolean isReady = false;
		TopicMetadata topicMeta = null;
		while (!isReady) {
			Random random = new Random();
			MockKafka kafka = kafkaCluster.get(random.nextInt(kafkaCluster.size()));
			topicMeta = kafka.fetchTopicMeta(topic);
			Seq<PartitionMetadata> partitionsMetadata = topicMeta.partitionsMetadata();
			Iterator<PartitionMetadata> iterator = partitionsMetadata.iterator();
			boolean hasGotLeader = true;
			while (iterator.hasNext()) {
				PartitionMetadata partitionMeta = iterator.next();
				hasGotLeader &= (!partitionMeta.leader().isEmpty());
				if (partitionMeta.leader().isEmpty()) {
					System.out.println("Partition leader is not ready, wait 1s.");
					break;
				}
			}
			isReady = hasGotLeader;
			if (!isReady) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
				}
			}
		}
		return topicMeta;
	}
}
