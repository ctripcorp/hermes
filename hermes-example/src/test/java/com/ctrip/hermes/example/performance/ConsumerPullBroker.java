package com.ctrip.hermes.example.performance;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.BeforeClass;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.broker.bootstrap.BrokerBootstrap;
import com.ctrip.hermes.consumer.api.MessageListener;
import com.ctrip.hermes.consumer.engine.Engine;
import com.ctrip.hermes.consumer.engine.Subscriber;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.producer.api.Producer;

/**
 * Test the performance on: consumer pull msgs from broker. Assume that network is not th bottleneck.
 */
public class ConsumerPullBroker extends ComponentTestCase {

	final static String TOPIC = "order_new";

	private static final int MESSAGE_COUNT = 20000;

	static AtomicInteger receiveCount = new AtomicInteger(0);

	@BeforeClass
	public static void beforeClass() {
		System.setProperty("devMode", "false");
	}

	@Test
	public void testConsume() throws Exception {

		produceMsgs();

		String topic = TOPIC;
		Engine engine = lookup(Engine.class);

		final long startTime = System.currentTimeMillis();
		Subscriber s = new Subscriber(topic, "group1", new MessageListener<String>() {
			@Override
			public void onMessage(List<ConsumerMessage<String>> msgs) {
				receiveCount.addAndGet(msgs.size());
				// System.out.println("receiveCount: " + receiveCount);
				if (receiveCount.get() >= MESSAGE_COUNT) {

					long endTime = System.currentTimeMillis();
					System.out.println(String.format("Result: Time: %.2f(s), msgs: %d, QPS: %.2f msg/s",
					      (endTime - startTime) / 1000f, MESSAGE_COUNT, MESSAGE_COUNT / ((endTime - startTime) / 1000f)));
					System.exit(0);
				}
			}
		});

		engine.start(s);

		System.in.read();
	}

	private void produceMsgs() throws Exception {
		startBroker();

		Producer p = Producer.getInstance();

		for (int i = 0; i < MESSAGE_COUNT; i++)
			p.message("order_new", "0", "hello").withRefKey("key").withPriority().send();
	}

	private void startBroker() throws Exception {
		new Thread() {
			public void run() {

				try {
					lookup(BrokerBootstrap.class).start();
				} catch (Exception e) {
					// todo: log!
					System.out.println("Fail to start Broker: " + e.getMessage());
				}
			}

			;
		}.start();

		Thread.sleep(2000);
	}
}
