package com.ctrip.hermes.example;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.BeforeClass;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.broker.bootstrap.BrokerBootstrap;
import com.ctrip.hermes.consumer.api.BaseMessageListener;
import com.ctrip.hermes.consumer.api.Consumer;
import com.ctrip.hermes.consumer.api.MessageListenerConfig;
import com.ctrip.hermes.consumer.engine.Engine;
import com.ctrip.hermes.consumer.engine.Subscriber;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.result.CompletionCallback;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.metrics.HttpMetricsServer;
import com.ctrip.hermes.producer.api.Producer;
import com.dianping.cat.Cat;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;

public class OneBoxTest extends ComponentTestCase {

	private Map<String, Map<String, Integer>> m_nacks = new HashMap<String, Map<String, Integer>>();

	@BeforeClass
	public static void beforeClass() {
		System.setProperty("devMode", "true");
	}

	@Test
	public void testProduce() throws Exception {
		// startBroker();
		Producer p = Producer.getInstance();

		Future<SendResult> future = p.message("order_new", "0", 1233213423L).withRefKey("key").withPriority().send();

		try {
			future.get();
			System.out.println("Send Success");
		} catch (Exception e) {
			System.out.println("Send Fail");
		}

		System.in.read();
	}

	@Test
	public void testConsumer() throws Exception {
		// startBroker();

		Thread.sleep(2000);
		Engine engine = lookup(Engine.class);
		final AtomicLong counter = new AtomicLong(0);

		String groupId = "group1";
		Subscriber s = new Subscriber("order_new", groupId, new BaseMessageListener<Long>() {

			@Override
			protected void onMessage(ConsumerMessage<Long> msg) {
				counter.incrementAndGet();
				// System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>Received: " + msg.getBody());
			}
		});
		engine.start(s);

		System.in.read();
	}

	@Test
	public void testProducePerformance() throws Exception {
		startBroker();

		final int times = 20000;
		int threadCount = 2;
		final String topic = "order_new";
		final CountDownLatch latch = new CountDownLatch(times * threadCount);
		Thread.sleep(2000);
		Producer p = Producer.getInstance();

		p.message(topic, "0", 1233213423L).withRefKey("key").withPriority().send();
		p.message(topic, "0", 1233213423L).withRefKey("key").withPriority().send();
		Thread.sleep(1000);

		long start = System.currentTimeMillis();

		for (int i = 0; i < threadCount; i++) {
			Thread t = new Thread() {
				public void run() {
					producePerformance(times, topic, latch);
				}
			};
			t.start();
		}

		// latch.await(30, TimeUnit.SECONDS);
		latch.await();

		long progressTime = System.currentTimeMillis() - start;
		System.out.println(String.format("%d Threads produce %d msgs spends %d ms, QPS: %.2f msg/s", threadCount, times
		      * threadCount, progressTime, (float) (times * threadCount) / (progressTime / 1000f)));

		System.in.read();
	}

	private void producePerformance(int times, String topic, final CountDownLatch latch) {
		Random random = new Random();

		for (int i = 0; i < times; i++) {
			String uuid = UUID.randomUUID().toString();
			String msg = uuid;

			boolean priority = random.nextBoolean();
			SettableFuture<SendResult> future;
			if (priority) {
				future = (SettableFuture<SendResult>) Producer.getInstance().message(topic, null, msg + " priority")
				      .withRefKey(uuid).withPriority().send();
			} else {
				future = (SettableFuture<SendResult>) Producer.getInstance().message(topic, null, msg + " non-priority")
				      .withRefKey(uuid).send();
			}

			future.addListener(new Runnable() {

				@Override
				public void run() {
					latch.countDown();
				}
			}, MoreExecutors.directExecutor());
		}
	}

	@Test
	public void test() throws Exception {
//		startBroker();

//		HttpMetricsServer server = new HttpMetricsServer("localhost", 9999);
//		server.start();

		String topic = "order_new";

		Map<String, List<String>> subscribers = new HashMap<String, List<String>>();
		// subscribers.put("group2", Arrays.asList("1-a"));
		subscribers.put("leo1", Arrays.asList("1-a"));
		// subscribers.put("group2", Arrays.asList("2-a", "2-b"));
		// subscribers.put("group3", Arrays.asList("3-a", "3-b", "3-c"));

		for (Map.Entry<String, List<String>> entry : subscribers.entrySet()) {
			String groupId = entry.getKey();
			Map<String, Integer> nacks = findNacks(groupId);
			for (String id : entry.getValue()) {
				MessageListenerConfig config = new MessageListenerConfig();
				// config.setStrictlyOrderingRetryPolicy(StrictlyOrderingRetryPolicy.evenRetry(3000, 3));
				System.out.println("Starting consumer " + groupId + ":" + id);
				Consumer.getInstance().start(topic, groupId, new MyConsumer(nacks, id), config);
			}

		}

		System.out.println("Starting producer...");
		// send(topic, "ACK-");

//		Random rnd = new Random();
		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		while (true) {
			String line = in.readLine();
//			String line = "";
//			Thread.sleep(rnd.nextInt(1000));
			String prefix = "ACK-";
			if ("q".equals(line)) {
				break;
			} else if (line.startsWith("n")) {
				int nackCnt = 1;
				try {
					nackCnt = Integer.parseInt(line.substring(1).trim());
				} catch (Exception e) {
				}

				prefix = "NACK-" + nackCnt + "-";

				send(topic, prefix);
			} else if (line.startsWith("c")) {
				String[] parts = line.split(" ");
				if (parts.length == 3) {
					String groupId = parts[1];
					String id = parts[2];
					Map<String, Integer> nacks = findNacks(groupId);
					System.out.println(String.format("Starting consumer with groupId %s and id %s", groupId, id));
					Consumer.getInstance().start(topic, groupId, new MyConsumer(nacks, id));
				}
			} else {
				send(topic, prefix);
			}

		}
	}

	private Map<String, Integer> findNacks(String groupId) {
		if (!m_nacks.containsKey(groupId)) {
			m_nacks.put(groupId, new ConcurrentHashMap<String, Integer>());
		}
		return m_nacks.get(groupId);
	}

	private void send(String topic, String prefix) throws Exception {
		String uuid = UUID.randomUUID().toString();
		String msg = prefix + uuid;
		System.out.println(">>> " + msg);
		Random random = new Random();

		boolean priority = random.nextBoolean();

		if (priority) {
			Producer.getInstance().message(topic, msg, msg + " priority").withRefKey(uuid).withPriority()
			      .setCallback(new CompletionCallback<SendResult>() {

				      @Override
				      public void onSuccess(SendResult result) {
					      System.out.println("Sent");
				      }

				      @Override
				      public void onFailure(Throwable t) {
					      System.out.println(t.getMessage());
				      }
			      }).send();
		} else {
			Producer.getInstance().message(topic, msg, msg + " non-priority").withRefKey(uuid).send();
		}

	}

	static class MyConsumer extends BaseMessageListener<String> {

		private Map<String, Integer> m_nacks;

		private String m_id;

		public MyConsumer(Map<String, Integer> nacks, String id) {
			m_nacks = nacks;
			m_id = id;
		}

		@Override
		public void onMessage(ConsumerMessage<String> msg) {
			String body = msg.getBody();
			System.out.println(m_id + "<<< " + body + " latency: " + (System.currentTimeMillis() - msg.getBornTime()));

			// TODO
			if (body.startsWith("NACK-")) {
				int totalNackCnt = Integer.parseInt(body.substring(5, body.indexOf("-", 5)));

				if (!m_nacks.containsKey(body)) {
					m_nacks.put(body, 1);
					msg.nack();
				} else {
					int curNackCnt = m_nacks.get(body);
					if (curNackCnt < totalNackCnt) {
						m_nacks.put(body, curNackCnt + 1);
						msg.nack();
					} else {
						m_nacks.remove(body);
						msg.ack();
					}
				}
			} else {
				msg.ack();
			}
		}

	}

	private void startBroker() throws Exception {

		lookup(BrokerBootstrap.class).start();
		Thread.sleep(2000);
	}
	
	@Test
	public void startBrokerOnly() throws Exception {
		HttpMetricsServer server = new HttpMetricsServer("localhost", 9998);
		server.start();
		
		lookup(BrokerBootstrap.class).start();
		System.in.read();
	}
}
