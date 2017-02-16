package com.ctrip.hermes.kafka;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.junit.Test;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.ctrip.hermes.consumer.api.BaseMessageListener;
import com.ctrip.hermes.consumer.api.Consumer;
import com.ctrip.hermes.consumer.api.Consumer.ConsumerHolder;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.kafka.avro.AvroVisitEvent;
import com.ctrip.hermes.kafka.avro.KafkaAvroTest;
import com.ctrip.hermes.metrics.HermesMetricsRegistry;
import com.ctrip.hermes.metrics.MetricsUtils;
import com.ctrip.hermes.producer.api.Producer;
import com.ctrip.hermes.producer.api.Producer.MessageHolder;

public class SimpleAvroInDevTest {

	@Test
	public void simpleAvroMessageTest() throws IOException {
		String topic = "kafka.SimpleAvroTopic";
		String group = "kafka.SimpleAvroTopic.group";
		MetricRegistry metrics = HermesMetricsRegistry.getMetricRegistryByT(topic);
		final Meter sent = metrics.meter("sent");
		final Meter received = metrics.meter("received");

		Producer producer = Producer.getInstance();

		ConsumerHolder consumer = Consumer.getInstance().start(topic, group, new BaseMessageListener<AvroVisitEvent>() {

			@Override
			protected void onMessage(ConsumerMessage<AvroVisitEvent> msg) {
				AvroVisitEvent body = msg.getBody();
				System.out.println("Receive: " + body);
				received.mark();
			}
		});

		System.out.println("Starting consumer...");

		try (BufferedReader in = new BufferedReader(new InputStreamReader(System.in))) {
			while (true) {
				String line = in.readLine();
				if ("q".equals(line)) {
					break;
				}

				AvroVisitEvent proMsg = KafkaAvroTest.generateEvent();
				MessageHolder holder = producer.message(topic, proMsg.getIp().toString(), proMsg);
				holder.send();
				System.out.println("Sent: " + proMsg);
				sent.mark();
			}
		}

		consumer.close();
		System.out.println(MetricsUtils.printMeter("sent", sent));
		System.out.println(MetricsUtils.printMeter("received", received));
	}
}
