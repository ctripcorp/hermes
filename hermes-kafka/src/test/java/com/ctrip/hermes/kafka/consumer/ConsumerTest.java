package com.ctrip.hermes.kafka.consumer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import org.junit.Test;

import com.ctrip.hermes.consumer.api.BaseMessageListener;
import com.ctrip.hermes.consumer.api.Consumer;
import com.ctrip.hermes.consumer.api.Consumer.ConsumerHolder;
import com.ctrip.hermes.consumer.api.MessageListener;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.producer.api.Producer;
import com.ctrip.hermes.producer.api.Producer.MessageHolder;

public class ConsumerTest {

	@Test
	public void testConsumerNotMessage() throws IOException {
		String topic = "kafka.SimpleTextTopic";
		String group = "SimpleTextTopicGroup";

		ConsumerHolder consumerHolder = Consumer.getInstance().start(topic, group, new BaseMessageListener<VisitEvent>() {

			@Override
			protected void onMessage(ConsumerMessage<VisitEvent> msg) {
				VisitEvent event = msg.getBody();
				System.out.println("Receive: " + event);
			}
		});

		System.out.println("Starting consumer...");
		BufferedReader in = null;
		try {
			in = new BufferedReader(new InputStreamReader(System.in));
			while (true) {
				String line = in.readLine();
				if ("q".equals(line)) {
					break;
				}
			}
		} finally {
			if (in != null) {
				in.close();
			}
		}

		consumerHolder.close();
	}

	@Test
	public void testOneConsumerOneGroup() throws IOException {
		String topic = "kafka.SimpleTextTopic";
		String group = "SimpleTextTopicGroup";

		Producer producer = Producer.getInstance();

		ConsumerHolder consumerHolder = Consumer.getInstance().start(topic, group, new BaseMessageListener<VisitEvent>() {

			@Override
			protected void onMessage(ConsumerMessage<VisitEvent> msg) {
				VisitEvent event = msg.getBody();
				System.out.println("Receive: " + event);
			}
		});

		System.out.println("Starting consumer...");
		BufferedReader in = null;
		try {
			in = new BufferedReader(new InputStreamReader(System.in));
			while (true) {
				String line = in.readLine();
				if ("q".equals(line)) {
					break;
				}

				VisitEvent event = ProducerTest.generateEvent();
				MessageHolder holder = producer.message(topic, null, event);
				holder.send();
				System.out.println("Sent: " + event);
			}
		} finally {
			if (in != null) {
				in.close();
			}
		}

		consumerHolder.close();
	}

	@Test
	public void testTwoConsumerOneGroup() throws IOException {
		String topic = "kafka.SimpleTextTopic";
		String group = "group1";

		Producer producer = Producer.getInstance();

		ConsumerHolder consumerHolder1 = Consumer.getInstance().start(topic, group, new MessageListener<VisitEvent>() {

			@Override
			public void onMessage(List<ConsumerMessage<VisitEvent>> msgs) {
				for (ConsumerMessage<VisitEvent> msg : msgs) {
					VisitEvent event = msg.getBody();
					System.out.println("Consumer1 Receive: " + event);
				}
			}
		});

		System.out.println("Starting consumer1...");

		ConsumerHolder consumerHolder2 = Consumer.getInstance().start(topic, group, new MessageListener<VisitEvent>() {

			@Override
			public void onMessage(List<ConsumerMessage<VisitEvent>> msgs) {
				for (ConsumerMessage<VisitEvent> msg : msgs) {
					VisitEvent event = msg.getBody();
					System.out.println("Consumer2 Receive: " + event);
				}
			}
		});

		System.out.println("Starting consumer2...");
		BufferedReader in = null;
		try {
			in = new BufferedReader(new InputStreamReader(System.in));
			while (true) {
				String line = in.readLine();
				if ("q".equals(line)) {
					break;
				}

				VisitEvent event = ProducerTest.generateEvent();
				MessageHolder holder = producer.message(topic, null, event);
				holder.send();
				System.out.println("Sent: " + event);
			}
		} finally {
			if (in != null) {
				in.close();
			}
		}

		consumerHolder1.close();
		consumerHolder2.close();
	}

	@Test
	public void testTwoConsumerTwoGroup() throws IOException {
		String topic = "kafka.SimpleTextTopic";
		String group1 = "group1";
		String group2 = "group2";

		Producer producer = Producer.getInstance();

		ConsumerHolder consumerHolder1 = Consumer.getInstance().start(topic, group1, new MessageListener<VisitEvent>() {

			@Override
			public void onMessage(List<ConsumerMessage<VisitEvent>> msgs) {
				for (ConsumerMessage<VisitEvent> msg : msgs) {
					VisitEvent event = msg.getBody();
					System.out.println("Consumer1 Receive: " + event);
				}
			}
		});

		System.out.println("Starting consumer1...");

		ConsumerHolder consumerHolder2 = Consumer.getInstance().start(topic, group2, new MessageListener<VisitEvent>() {

			@Override
			public void onMessage(List<ConsumerMessage<VisitEvent>> msgs) {
				for (ConsumerMessage<VisitEvent> msg : msgs) {
					VisitEvent event = msg.getBody();
					System.out.println("Consumer2 Receive: " + event);
				}
			}
		});

		System.out.println("Starting consumer2...");
		BufferedReader in = null;
		try {
			in = new BufferedReader(new InputStreamReader(System.in));
			while (true) {
				String line = in.readLine();
				if ("q".equals(line)) {
					break;
				}

				VisitEvent event = ProducerTest.generateEvent();
				MessageHolder holder = producer.message(topic, null, event);
				holder.send();
				System.out.println("Sent: " + event);
			}
		} finally {
			if (in != null) {
				in.close();
			}
		}

		consumerHolder1.close();
		consumerHolder2.close();
	}
}
