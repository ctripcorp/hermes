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
import com.ctrip.hermes.metrics.HermesMetricsRegistry;
import com.ctrip.hermes.metrics.MetricsUtils;
import com.ctrip.hermes.producer.api.Producer;
import com.ctrip.hermes.producer.api.Producer.MessageHolder;

public class SimpleTextInUatTest {

	@Test
	public void simpleTextMessageTest() throws IOException {
		String topic = "PerfTest";
		String group = "group.PerfTest";
		MetricRegistry metrics = HermesMetricsRegistry.getMetricRegistryByT(topic);
		final Meter sent = metrics.meter("sent");
		final Meter received = metrics.meter("received");

		Producer producer = Producer.getInstance();

		ConsumerHolder consumer = Consumer.getInstance().start(topic, group, new BaseMessageListener<String>() {

			@Override
			protected void onMessage(ConsumerMessage<String> msg) {
				String body = msg.getBody();
				System.out.println("Receive: " + body);
				received.mark();
			}
		});

		System.out.println("Starting consumer...");

		int count = 1;
		try (BufferedReader in = new BufferedReader(new InputStreamReader(System.in))) {
			while (true) {
				String line = in.readLine();
				if ("q".equals(line)) {
					break;
				}

				String proMsg = "Hello Ctrip " + count++;
				MessageHolder holder = producer.message(topic, String.valueOf(System.currentTimeMillis()), proMsg);
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
