package com.ctrip.hermes.consumer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.consumer.api.BaseMessageListener;
import com.ctrip.hermes.consumer.api.Consumer;
import com.ctrip.hermes.consumer.api.Consumer.ConsumerHolder;
import com.ctrip.hermes.consumer.message.BrokerConsumerMessage;
import com.ctrip.hermes.core.message.ConsumerMessage;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class StartConsumer extends ComponentTestCase {

	@Test
	public void testQueryMessageOffset() throws Exception {
		System.out.println(Consumer.getInstance().getOffsetByTime("song.test", 0, Long.MIN_VALUE));
	}

	@Test
	public void test() throws Exception {

		Map<Pair<String, String>, List<Pair<String, ConsumerHolder>>> topicGroup2Consumers = new HashMap<Pair<String, String>, List<Pair<String, ConsumerHolder>>>();

		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		while (true) {
			String line = in.readLine();

			if ("quit".equals(line)) {
				return;
			} else if (line.startsWith("start consumer ")) {
				createConsumer(topicGroup2Consumers, line);
			} else if (line.equals("list consumers")) {
				listConsumer(topicGroup2Consumers);
			} else if (line.startsWith("stop consumer ")) {
				stopConsumer(topicGroup2Consumers, line);
			} else if (line.equals("status")) {
				printAllThreads();
			}

		}

	}

	private void stopConsumer(Map<Pair<String, String>, List<Pair<String, ConsumerHolder>>> topicGroup2Consumers,
	      String line) {
		String[] args = line.split(" ");
		if (args.length != 5) {
			System.out.println("[ERROR]Command format: stop consumer {topic} {consumer-group} {name}");
		} else {
			String topic = args[2];
			String consumerGroup = args[3];
			String name = args[4];
			Pair<String, String> topicGroup = new Pair<String, String>(topic, consumerGroup);

			if (topicGroup2Consumers.containsKey(topicGroup)) {
				ConsumerHolder consumerHolder = null;
				for (Pair<String, ConsumerHolder> consumer : topicGroup2Consumers.get(topicGroup)) {
					if (name.equals(consumer.getKey())) {
						consumerHolder = consumer.getValue();
						break;
					}
				}
				if (consumerHolder != null) {
					consumerHolder.close();

					System.out.println(String.format("Consumer stopped(topic=%s, consumer-group=%s, name=%s)", topic,
					      consumerGroup, name));
				} else {
					System.out.println(String.format("Can not find consumer(topic=%s, consumer-group=%s, name=%s)", topic,
					      consumerGroup, name));
				}
			} else {
				System.out.println(String.format("Can not find consumer(topic=%s, consumer-group=%s, name=%s)", topic,
				      consumerGroup, name));
			}
		}

	}

	private void listConsumer(Map<Pair<String, String>, List<Pair<String, ConsumerHolder>>> topicGroup2Consumers) {
		StringBuilder sb = new StringBuilder();

		sb.append("Consumers:\n");
		for (Map.Entry<Pair<String, String>, List<Pair<String, ConsumerHolder>>> entry : topicGroup2Consumers.entrySet()) {
			sb.append(String.format("[Topic:%s, ConsumerGroup:%s]", entry.getKey().getKey(), entry.getKey().getValue()));
			sb.append("{");
			for (Pair<String, ConsumerHolder> consumer : entry.getValue()) {
				sb.append(consumer.getKey()).append(",");
			}
			sb.deleteCharAt(sb.length() - 1).append("}");
		}

		System.out.println(sb.toString());

	}

	private void createConsumer(Map<Pair<String, String>, List<Pair<String, ConsumerHolder>>> topicGroup2Consumers,
	      String line) {
		String[] args = line.split(" ");
		if (args.length < 4) {
			System.out.println("[ERROR]Command format: create consumer {topic} {consumer-group} ({nack-times})");
		} else {
			String topic = args[2];
			String consumerGroup = args[3];
			Pair<String, String> topicGroup = new Pair<String, String>(topic, consumerGroup);
			if (!topicGroup2Consumers.containsKey(topicGroup)) {
				topicGroup2Consumers.put(topicGroup, new ArrayList<Pair<String, ConsumerHolder>>());
			}
			int id = topicGroup2Consumers.get(topicGroup).size();
			String name = topic + "_" + consumerGroup + "_" + id;

			if (args.length == 4) {

				topicGroup2Consumers.get(topicGroup).add(
				      new Pair<String, ConsumerHolder>(name, Consumer.getInstance().start(topic, consumerGroup,
				            new AckMessageListener(name))));

				System.out.println(String.format("Ack consumer started(topic=%s, consumer-group=%s, name=%s)", topic,
				      consumerGroup, name));
			} else if (args.length >= 5) {
				int nackTimes = Integer.valueOf(args[4]);

				topicGroup2Consumers.get(topicGroup).add(
				      new Pair<String, ConsumerHolder>(name, Consumer.getInstance().start(topic, consumerGroup,
				            new NAckMessageListener(name, nackTimes))));

				System.out.println(String.format(
				      "Nack consumer started(topic=%s, consumer-group=%s, name=%s, nackTimes=%s)", topic, consumerGroup,
				      name, nackTimes));
			}
		}
	}

	private void printAllThreads() {
		Map<Thread, StackTraceElement[]> allStackTraces = Thread.getAllStackTraces();
		Map<String, List<String>> groups = new HashMap<String, List<String>>();
		for (Thread thread : allStackTraces.keySet()) {
			String group = thread.getThreadGroup().getName();
			if (!groups.containsKey(group)) {
				groups.put(group, new ArrayList<String>());
			}
			groups.get(group).add(thread.getName());
		}

		for (Map.Entry<String, List<String>> entry : groups.entrySet()) {
			System.out.println(String.format("Thread Group %s: threads: %s", entry.getKey(), entry.getValue()));
		}
	}

	static class AckMessageListener extends BaseMessageListener<String> {

		private String m_name;

		private AtomicLong m_count = new AtomicLong(0);

		public AckMessageListener(String name) {
			m_name = name;
		}

		@Override
		public void onMessage(ConsumerMessage<String> msg) {

			if (m_count.incrementAndGet() % 1000 == 0) {
				System.out.println("Received 1000 msgs.");
			}
			System.out.println(String.format("[%s]Message received(topic:%s, body:%s, partition:%s, priority:%s)", m_name,
			      msg.getTopic(), msg.getBody(), ((BrokerConsumerMessage<String>) msg).getPartition(),
			      ((BrokerConsumerMessage<String>) msg).isPriority()));
			msg.ack();
		}
	}

	static class NAckMessageListener extends BaseMessageListener<String> {

		private String m_name;

		private int m_nackTimes;

		private ConcurrentMap<String, AtomicInteger> m_nacks = new ConcurrentHashMap<String, AtomicInteger>();

		public NAckMessageListener(String name, int nackTimes) {
			m_name = name;
			m_nackTimes = nackTimes;
		}

		@Override
		public void onMessage(ConsumerMessage<String> msg) {

			m_nacks.putIfAbsent(msg.getRefKey(), new AtomicInteger(m_nackTimes));

			System.out.println(String.format("[%s]Message received(topic:%s, body:%s, partition:%s, priority:%s)", m_name,
			      msg.getTopic(), msg.getBody(), ((BrokerConsumerMessage<String>) msg).getPartition(),
			      ((BrokerConsumerMessage<String>) msg).isPriority()));

			AtomicInteger nackTimes = m_nacks.get(msg.getRefKey());

			if (nackTimes.get() == 0) {
				m_nacks.remove(msg.getRefKey());
				msg.ack();
			} else {
				nackTimes.decrementAndGet();
				msg.nack();
			}
		}
	}

	@Test
	public void testConsumer() throws Exception {
		Consumer.getInstance().start("yq.test2", "yq.test", new BaseMessageListener<String>() {

			@Override
			protected void onMessage(ConsumerMessage<String> msg) {
				System.out.println("RECEIVED MESSAGE: " + msg.getBody());
			}
		});
		System.out.println("Start success");
		System.in.read();
	}

}
