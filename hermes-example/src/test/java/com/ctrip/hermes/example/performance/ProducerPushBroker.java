package com.ctrip.hermes.example.performance;

import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.broker.bootstrap.BrokerBootstrap;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.producer.api.Producer;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;

/**
 * Test the performance on: producer send msgs to broker. Assume that network is not th bottleneck.
 */
public class ProducerPushBroker extends ComponentTestCase {

	final static int MESSAGE_COUNT = 20000;

	@BeforeClass
	public static void beforeClass() {
		System.setProperty("devMode", "false");
	}

	@Test
	public void testMysqlBroker() throws Exception {
		startBroker();

		final CountDownLatch latch = new CountDownLatch(MESSAGE_COUNT);

		Producer p = Producer.getInstance();

		p.message("order_new", "0", 1233213423L).withRefKey("key").withPriority().send();

		long startTime = System.currentTimeMillis();
		for (int i = 0; i < MESSAGE_COUNT; i++) {
			SettableFuture<SendResult> future = (SettableFuture<SendResult>) p.message("order_new", "0", 1233213423L)
			      .withRefKey("key").withPriority().send();

			future.addListener(new Runnable() {

				@Override
				public void run() {
					latch.countDown();
				}
			}, MoreExecutors.sameThreadExecutor());
		}

		boolean isDone = latch.await(30, TimeUnit.SECONDS);
		assertTrue(isDone);

		long endTime = System.currentTimeMillis();
		outputResult(endTime - startTime);

	}

	private void outputResult(long eclipseTime) {
		System.out.println(String.format("Produce %d msgs spends %d ms, QPS: %.2f msg/s", MESSAGE_COUNT, eclipseTime,
		      MESSAGE_COUNT / (eclipseTime / 1000f)));
	}

	@Test
	public void testKafkaBroker() {

	}

	private void startBroker() throws Exception {
		new Thread() {
			public void run() {

				try {
					lookup(BrokerBootstrap.class).start();
				} catch (Exception e) {
					System.out.println("Fail to start Broker: " + e.getMessage());
				}
			}
		}.start();

		Thread.sleep(2000);
	}
}
