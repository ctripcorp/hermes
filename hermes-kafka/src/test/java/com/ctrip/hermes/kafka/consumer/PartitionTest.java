package com.ctrip.hermes.kafka.consumer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import org.junit.Test;

import com.ctrip.hermes.consumer.api.Consumer;
import com.ctrip.hermes.consumer.api.Consumer.ConsumerHolder;
import com.ctrip.hermes.consumer.api.MessageListener;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.producer.api.Producer;
import com.ctrip.hermes.producer.api.Producer.MessageHolder;

public class PartitionTest {

	@Test
	public void testTwoPartitionOneConsumer() throws IOException {
		String topicPattern = "kafka.SimpleTextTopic";
		String group = "SimpleTextTopicGroup";

		Producer producer = Producer.getInstance();

		ConsumerHolder consumerHolder = Consumer.getInstance().start(topicPattern, group,
		      new MessageListener<VisitEvent>() {

			      @Override
			      public void onMessage(List<ConsumerMessage<VisitEvent>> msgs) {
				      for (ConsumerMessage<VisitEvent> msg : msgs) {
					      VisitEvent event = msg.getBody();
					      System.out.println(String.format("Receive from %s: %s", msg.getTopic(), event));
				      }
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
				MessageHolder holder = producer.message(topicPattern, null, event);
				holder.send();
				System.out.println(String.format("Sent to %s: %s", topicPattern, event));
			}
		} finally {
			if (in != null) {
				in.close();
			}
		}
		consumerHolder.close();
	}

	@Test
	public void testTwoPartitionTwoConsumer() throws IOException {
		String topicPattern = "kafka.SimpleTextTopic";
		String group = "SimpleTextTopicGroup";

		Producer producer = Producer.getInstance();

		ConsumerHolder consumerHolder1 = Consumer.getInstance().start(topicPattern, group,
		      new MessageListener<VisitEvent>() {

			      @Override
			      public void onMessage(List<ConsumerMessage<VisitEvent>> msgs) {
				      for (ConsumerMessage<VisitEvent> msg : msgs) {
					      VisitEvent event = msg.getBody();
					      System.out.println(String.format("Consumer1 Receive from %s: %s", msg.getTopic(), event));
				      }
			      }
		      });

		System.out.println("Starting consumer1...");

		ConsumerHolder consumerHolder2 = Consumer.getInstance().start(topicPattern, group,
		      new MessageListener<VisitEvent>() {

			      @Override
			      public void onMessage(List<ConsumerMessage<VisitEvent>> msgs) {
				      for (ConsumerMessage<VisitEvent> msg : msgs) {
					      VisitEvent event = msg.getBody();
					      System.out.println(String.format("Consumer2 Receive from %s: %s", msg.getTopic(), event));
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
				MessageHolder holder = producer.message(topicPattern, null, event);
				holder.send();
				System.out.println(String.format("Sent to %s: %s", topicPattern, event));
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
