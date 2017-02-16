package com.ctrip.hermes.kafka.admin;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Properties;

import kafka.admin.AdminUtils;
import kafka.api.TopicMetadata;
import kafka.utils.ZkUtils;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.junit.Test;

public class TopicTest {

	@Test
	public void createTopicInTestEnv() {
		String ZOOKEEPER_CONNECT = "";
		ZkClient zkClient = new ZkClient(new ZkConnection(ZOOKEEPER_CONNECT));
		ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(ZOOKEEPER_CONNECT), false);
		zkClient.setZkSerializer(new ZKStringSerializer());
		int partition = 1;
		int replication = 1;
		String topic = String.format("kafka.test_create_topic_p%s_r%s", partition, replication);
		if (AdminUtils.topicExists(zkUtils, topic)) {
			TopicMetadata topicMetadata = AdminUtils.fetchTopicMetadataFromZk(topic, zkUtils);
			System.out.println(topicMetadata);
			AdminUtils.deleteTopic(zkUtils, topic);
		}
		AdminUtils.createTopic(zkUtils, topic, partition, replication, new Properties());
	}

	@Test
	public void testSerializable() throws IOException {
		// Object serializable = new String("{\"version\":1,\"partitions\":{\"0\":[1]}}");
		Object serializable = new String("1");
		ByteArrayOutputStream byteArrayOS = new ByteArrayOutputStream();
		ObjectOutputStream stream = new ObjectOutputStream(byteArrayOS);
		stream.writeObject(serializable);
		stream.close();
		byte[] result = byteArrayOS.toByteArray();
		System.out.println(Arrays.toString(result));
	}
}
