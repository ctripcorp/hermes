package com.ctrip.hermes.kafka.consumer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import com.ctrip.hermes.consumer.api.Consumer;
import com.ctrip.hermes.consumer.api.Consumer.ConsumerHolder;
import com.ctrip.hermes.consumer.api.MessageListener;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.producer.api.Producer;
import com.ctrip.hermes.producer.api.Producer.MessageHolder;

public class WildcardTopicsTest {

	@Test
	public void testWildcardTopics() throws IOException {
		String topicPattern = "kafka.SimpleTextTopic.*";
		String sendTopic1 = "kafka.SimpleTextTopic1";
		String sendTopic2 = "kafka.SimpleTextTopic2";
		String group = "SimpleTextTopic1Group";

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

		Random random = new Random();
		BufferedReader in = null;
		try {
			in = new BufferedReader(new InputStreamReader(System.in));
			while (true) {
				String line = in.readLine();
				if ("q".equals(line)) {
					break;
				}

				VisitEvent event = ProducerTest.generateEvent();
				String topic = random.nextBoolean() ? sendTopic1 : sendTopic2;
				MessageHolder holder = producer.message(topic, null, event);
				holder.send();
				System.out.println(String.format("Sent to %s: %s", topic, event));
			}
		} finally {
			if (in != null) {
				in.close();
			}
		}
		consumerHolder.close();
	}
}
