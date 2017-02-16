package com.ctrip.hermes.kafka.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutionException;

import kafka.api.TopicMetadata;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ctrip.hermes.consumer.api.BaseMessageListener;
import com.ctrip.hermes.consumer.api.Consumer;
import com.ctrip.hermes.consumer.api.Consumer.ConsumerHolder;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.kafka.producer.KafkaFuture;
import com.ctrip.hermes.kafka.producer.KafkaMessageSender;
import com.ctrip.hermes.kafka.producer.KafkaSendResult;
import com.ctrip.hermes.kafka.server.MockKafkaCluster;
import com.ctrip.hermes.kafka.server.MockZookeeper;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.producer.api.Producer;
import com.ctrip.hermes.producer.api.Producer.MessageHolder;
import com.ctrip.hermes.producer.sender.MessageSender;

public class KafkaServiceTest {

	private static MockZookeeper zk;

	private static MockKafkaCluster kafkaCluster;

	private static final int CONSUMER_WAIT_BEFORE_READY = 10000;

	@BeforeClass
	public static void beforeClass() {
		zk = new MockZookeeper();
		kafkaCluster = new MockKafkaCluster(zk, 3);
	}

	@AfterClass
	public static void afterClass() {
		kafkaCluster.stop();
		zk.stop();
	}

	@Test
	public void resetToEarliestTest() throws IOException, InterruptedException, ExecutionException {
		String topic = "kafka.SimpleTextTopic1";
		kafkaCluster.createTopic(topic, 3, 1);
		TopicMetadata topicMeta = kafkaCluster.waitTopicUntilReady(topic);
		System.out.println(topicMeta);
		String group = "SimpleTextTopic1Group";

		List<String> expected = new ArrayList<String>();
		expected.add("abc");
		expected.add("DEF");
		expected.add("#$%");
		expected.add(" 23");
		expected.add("+- ");
		expected.add(" # ");

		final List<String> actual = new ArrayList<String>();

		Producer producer = Producer.getInstance();

		ConsumerHolder consumer = Consumer.getInstance().start(topic, group, new BaseMessageListener<String>() {

			@Override
			protected void onMessage(ConsumerMessage<String> msg) {
				String body = msg.getBody();
				actual.add(body);
				System.out.println("Receive1: " + body);
			}
		});

		System.out.println("Starting consumer...");
		Thread.sleep(CONSUMER_WAIT_BEFORE_READY);

		for (int i = 0; i < expected.size(); i++) {
			String proMsg = expected.get(i);

			MessageHolder holder = producer.message(topic, String.valueOf(i), proMsg);
			KafkaFuture future = (KafkaFuture) holder.send();
			KafkaSendResult result = future.get();
			System.out.println(String.format("Sent:%s, Partition:%s, Offset:%s", proMsg, result.getPartition(),
			      result.getOffset()));
		}

		int sleepCount = 0;
		while (actual.size() < expected.size() && sleepCount++ < 50) {
			Thread.sleep(100);
		}

		consumer.close();
		Assert.assertEquals(expected.size(), actual.size());
		Assert.assertEquals(new HashSet<String>(expected), new HashSet<String>(actual));

		KafkaService kafkaService = PlexusComponentLocator.lookup(KafkaService.class);
		kafkaService.resetConsumerOffset(topic, group, KafkaService.RESET_POSITION.EARLIEST);

		consumer = Consumer.getInstance().start(topic, group, new BaseMessageListener<String>() {

			@Override
			protected void onMessage(ConsumerMessage<String> msg) {
				String body = msg.getBody();
				actual.add(body);
				System.out.println("Receive1: " + body);
			}
		});

		System.out.println("Starting consumer...");
		Thread.sleep(CONSUMER_WAIT_BEFORE_READY);

		sleepCount = 0;
		while (actual.size() < expected.size() * 2 && sleepCount++ < 50) {
			Thread.sleep(100);
		}

		consumer.close();
		Assert.assertEquals(expected.size() * 2, actual.size());
		Assert.assertEquals(new HashSet<String>(expected), new HashSet<String>(actual));

		KafkaMessageSender kafkaSender = (KafkaMessageSender) PlexusComponentLocator.lookup(MessageSender.class,
		      Endpoint.KAFKA);
		kafkaSender.close();
	}
	
	@Test
	public void resetToLargestTest() throws IOException, InterruptedException, ExecutionException {
		String topic = "kafka.SimpleTextTopic2";
		kafkaCluster.createTopic(topic, 3, 1);
		TopicMetadata topicMeta = kafkaCluster.waitTopicUntilReady(topic);
		System.out.println(topicMeta);
		String group = "SimpleTextTopic2Group";

		List<String> expected = new ArrayList<String>();
		expected.add("abc");
		expected.add("DEF");
		expected.add("#$%");
		expected.add(" 23");
		expected.add("+- ");
		expected.add(" # ");

		final List<String> actual = new ArrayList<String>();

		Producer producer = Producer.getInstance();

		ConsumerHolder consumer = Consumer.getInstance().start(topic, group, new BaseMessageListener<String>() {

			@Override
			protected void onMessage(ConsumerMessage<String> msg) {
				String body = msg.getBody();
				actual.add(body);
				System.out.println("Receive1: " + body);
			}
		});

		System.out.println("Starting consumer...");
		Thread.sleep(CONSUMER_WAIT_BEFORE_READY);

		for (int i = 0; i < expected.size(); i++) {
			String proMsg = expected.get(i);

			MessageHolder holder = producer.message(topic, String.valueOf(i), proMsg);
			KafkaFuture future = (KafkaFuture) holder.send();
			KafkaSendResult result = future.get();
			System.out.println(String.format("Sent:%s, Partition:%s, Offset:%s", proMsg, result.getPartition(),
			      result.getOffset()));
		}

		int sleepCount = 0;
		while (actual.size() < expected.size() && sleepCount++ < 50) {
			Thread.sleep(100);
		}

		consumer.close();
		Assert.assertEquals(expected.size(), actual.size());
		Assert.assertEquals(new HashSet<String>(expected), new HashSet<String>(actual));

		for (int i = 0; i < expected.size(); i++) {
			String proMsg = expected.get(i);

			MessageHolder holder = producer.message(topic, String.valueOf(i), proMsg);
			KafkaFuture future = (KafkaFuture) holder.send();
			KafkaSendResult result = future.get();
			System.out.println(String.format("Sent:%s, Partition:%s, Offset:%s", proMsg, result.getPartition(),
			      result.getOffset()));
		}
		
		KafkaService kafkaService = PlexusComponentLocator.lookup(KafkaService.class);
		kafkaService.resetConsumerOffset(topic, group, KafkaService.RESET_POSITION.LATEST);

		consumer = Consumer.getInstance().start(topic, group, new BaseMessageListener<String>() {

			@Override
			protected void onMessage(ConsumerMessage<String> msg) {
				String body = msg.getBody();
				actual.add(body);
				System.out.println("Receive1: " + body);
			}
		});

		System.out.println("Starting consumer...");
		Thread.sleep(CONSUMER_WAIT_BEFORE_READY);

		for (int i = 0; i < expected.size(); i++) {
			String proMsg = expected.get(i);

			MessageHolder holder = producer.message(topic, String.valueOf(i), proMsg);
			KafkaFuture future = (KafkaFuture) holder.send();
			KafkaSendResult result = future.get();
			System.out.println(String.format("Sent:%s, Partition:%s, Offset:%s", proMsg, result.getPartition(),
			      result.getOffset()));
		}
		
		sleepCount = 0;
		while (actual.size() < expected.size() * 2 && sleepCount++ < 50) {
			Thread.sleep(100);
		}

		consumer.close();
		Assert.assertEquals(expected.size() * 2, actual.size());
		Assert.assertEquals(new HashSet<String>(expected), new HashSet<String>(actual));

		KafkaMessageSender kafkaSender = (KafkaMessageSender) PlexusComponentLocator.lookup(MessageSender.class,
		      Endpoint.KAFKA);
		kafkaSender.close();
	}
}
