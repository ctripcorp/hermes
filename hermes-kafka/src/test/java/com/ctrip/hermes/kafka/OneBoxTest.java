package com.ctrip.hermes.kafka;

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

public class OneBoxTest {

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
		if (kafkaCluster != null) {
			kafkaCluster.stop();
		}
		if (zk != null) {
			zk.stop();
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void simpleTextOneProducerWrongConsumerTest() throws InterruptedException, ExecutionException {
		String topic = "kafka.SimpleTextTopic1";
		String group = "NonExistGroup";

		ConsumerHolder consumerHolder = Consumer.getInstance().start(topic, group, new BaseMessageListener<String>() {

			@Override
			protected void onMessage(ConsumerMessage<String> msg) {
				String body = msg.getBody();
				System.out.println("Receive: " + body);
			}
		});

		consumerHolder.close();
	}

	@Test
	public void simpleTextOneProducerOneConsumerTest() throws IOException, InterruptedException, ExecutionException {
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
		KafkaMessageSender kafkaSender = (KafkaMessageSender) PlexusComponentLocator.lookup(MessageSender.class,
		      Endpoint.KAFKA);
		kafkaSender.close();
		Assert.assertEquals(expected.size(), actual.size());
		Assert.assertEquals(new HashSet<String>(expected), new HashSet<String>(actual));
	}

	/**
	 * Could not guarantee the total order of consumer side
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	@Test
	public void simpleTextOneProducerMultipleConsumerInOneGroupTest() throws IOException, InterruptedException,
	      ExecutionException {
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

		ConsumerHolder consumer1 = Consumer.getInstance().start(topic, group, new BaseMessageListener<String>() {

			@Override
			protected void onMessage(ConsumerMessage<String> msg) {
				String body = msg.getBody();
				actual.add(body);
				System.out.println("Receive1: " + body);
			}
		});

		System.out.println("Starting consumer1");
		Thread.sleep(CONSUMER_WAIT_BEFORE_READY);

		ConsumerHolder consumer2 = Consumer.getInstance().start(topic, group, new BaseMessageListener<String>() {

			@Override
			protected void onMessage(ConsumerMessage<String> msg) {
				String body = msg.getBody();
				actual.add(body);
				System.out.println("Receive2: " + body);
			}
		});
		System.out.println("Starting consumer2");
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
		while (actual.size() < expected.size() && sleepCount++ < 100) {
			Thread.sleep(100);
		}

		consumer1.close();
		consumer2.close();
		KafkaMessageSender kafkaSender = (KafkaMessageSender) PlexusComponentLocator.lookup(MessageSender.class,
		      Endpoint.KAFKA);
		kafkaSender.close();
		Assert.assertEquals(expected.size(), actual.size());
		Assert.assertEquals(new HashSet<String>(expected), new HashSet<String>(actual));
	}

	@Test
	public void simpleTextOneProducerMultipleConsumerInMultipleGroupTest() throws IOException, InterruptedException,
	      ExecutionException {
		String topic = "kafka.SimpleTextTopic3";
		kafkaCluster.createTopic(topic, 3, 1);
		TopicMetadata topicMeta = kafkaCluster.waitTopicUntilReady(topic);
		System.out.println(topicMeta);
		String group1 = "SimpleTextTopic3Group1";
		String group2 = "SimpleTextTopic3Group2";

		List<String> expected = new ArrayList<String>();
		expected.add("abc");
		expected.add("DEF");
		expected.add("#$%");
		expected.add(" 23");
		expected.add("+- ");
		expected.add(" # ");

		final List<String> actual1 = new ArrayList<String>();
		final List<String> actual2 = new ArrayList<String>();

		Producer producer = Producer.getInstance();

		ConsumerHolder consumer1 = Consumer.getInstance().start(topic, group1, new BaseMessageListener<String>() {

			@Override
			protected void onMessage(ConsumerMessage<String> msg) {
				String body = msg.getBody();
				actual1.add(body);
				System.out.println("Receive1: " + body);
			}
		});

		System.out.println("Starting consumer1");
		Thread.sleep(CONSUMER_WAIT_BEFORE_READY);

		ConsumerHolder consumer2 = Consumer.getInstance().start(topic, group2, new BaseMessageListener<String>() {

			@Override
			protected void onMessage(ConsumerMessage<String> msg) {
				String body = msg.getBody();
				actual2.add(body);
				System.out.println("Receive2: " + body);
			}
		});
		System.out.println("Starting consumer2");
		Thread.sleep(15000);

		for (int i = 0; i < expected.size(); i++) {
			String proMsg = expected.get(i);

			MessageHolder holder = producer.message(topic, String.valueOf(i), proMsg);
			KafkaFuture future = (KafkaFuture) holder.send();
			KafkaSendResult result = future.get();
			System.out.println(String.format("Sent:%s, Partition:%s, Offset:%s", proMsg, result.getPartition(),
			      result.getOffset()));
		}

		int sleepCount = 0;
		while ((actual1.size() < expected.size() || actual2.size() < expected.size()) && sleepCount++ < 50) {
			Thread.sleep(100);
		}

		consumer1.close();
		consumer2.close();
		KafkaMessageSender kafkaSender = (KafkaMessageSender) PlexusComponentLocator.lookup(MessageSender.class,
		      Endpoint.KAFKA);
		kafkaSender.close();
		Assert.assertEquals(expected.size(), actual1.size());
		Assert.assertEquals(expected.size(), actual2.size());
		Assert.assertEquals(new HashSet<String>(expected), new HashSet<String>(actual1));
		Assert.assertEquals(new HashSet<String>(expected), new HashSet<String>(actual2));
	}

	@Test
	public void simpleTextMultipleProducerOneConsumerTest() throws IOException, InterruptedException, ExecutionException {
		final String topic = "kafka.SimpleTextTopic4";
		kafkaCluster.createTopic(topic, 3, 1);
		TopicMetadata topicMeta = kafkaCluster.waitTopicUntilReady(topic);
		System.out.println(topicMeta);
		final String group = "SimpleTextTopic4Group";

		final List<String> expected = new ArrayList<String>();
		expected.add("abc");
		expected.add("DEF");
		expected.add("#$%");
		expected.add(" 23");
		expected.add("+- ");
		expected.add(" # ");

		final List<String> actual = new ArrayList<String>();

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

		Thread producer1 = new Thread() {
			public void run() {
				Producer producer = Producer.getInstance();
				for (int i = 0; i < expected.size(); i++) {
					String proMsg = expected.get(i);

					MessageHolder holder = producer.message(topic, String.valueOf(i), proMsg);
					KafkaFuture future = (KafkaFuture) holder.send();
					KafkaSendResult result;
					try {
						result = future.get();
						System.out.println(String.format("Sent1:%s, Partition:%s, Offset:%s", proMsg, result.getPartition(),
						      result.getOffset()));
					} catch (InterruptedException e) {
						e.printStackTrace();
					} catch (ExecutionException e) {
						e.printStackTrace();
					}
				}
			}
		};
		producer1.start();

		Thread producer2 = new Thread() {
			public void run() {
				Producer producer = Producer.getInstance();
				for (int i = 0; i < expected.size(); i++) {
					String proMsg = expected.get(i);

					MessageHolder holder = producer.message(topic, String.valueOf(i), proMsg);
					KafkaFuture future = (KafkaFuture) holder.send();
					KafkaSendResult result;
					try {
						result = future.get();
						System.out.println(String.format("Sent2:%s, Partition:%s, Offset:%s", proMsg, result.getPartition(),
						      result.getOffset()));
					} catch (InterruptedException e) {
						e.printStackTrace();
					} catch (ExecutionException e) {
						e.printStackTrace();
					}
				}
			}
		};
		producer2.start();

		int sleepCount = 0;
		while (actual.size() < expected.size() * 2 && sleepCount++ < 50) {
			Thread.sleep(100);
		}

		consumer.close();
		KafkaMessageSender kafkaSender = (KafkaMessageSender) PlexusComponentLocator.lookup(MessageSender.class,
		      Endpoint.KAFKA);
		kafkaSender.close();
		Assert.assertEquals(expected.size() * 2, actual.size());
	}
}
