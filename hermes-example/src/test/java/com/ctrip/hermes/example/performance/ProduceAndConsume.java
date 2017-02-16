package com.ctrip.hermes.example.performance;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.broker.bootstrap.BrokerBootstrap;
import com.ctrip.hermes.consumer.api.MessageListener;
import com.ctrip.hermes.consumer.engine.Engine;
import com.ctrip.hermes.consumer.engine.Subscriber;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.producer.api.Producer;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;

public class ProduceAndConsume extends ComponentTestCase {

	static AtomicInteger sendCount = new AtomicInteger(0);

	static AtomicInteger receiveCount = new AtomicInteger(0);

	static AtomicInteger totalSend = new AtomicInteger(0);

	static AtomicInteger totalReceive = new AtomicInteger(0);

	final static long timeInterval = 3000;

	final static String TOPIC = "order_new";

	final static int threadNum = 1;

	private void printAndClean() {
		int secondInTimeInterval = (int) timeInterval / 1000;

		totalSend.addAndGet(sendCount.get());
		totalReceive.addAndGet(receiveCount.get());
		System.out.println(String.format(
		      "Throughput:Send:%8d items (QPS: %.2f msg/s), Receive: %8d items (QPS: %.2f msg/s) " + "in %d "
		            + "second. " + "Total Send: %8d, Total Receive: %8d, Delta: %8d.", sendCount.get(), sendCount.get()
		            / (float) secondInTimeInterval, receiveCount.get(),
		      receiveCount.get() / (float) secondInTimeInterval, secondInTimeInterval, totalSend.get(),
		      totalReceive.get(), Math.abs(totalSend.get() - totalReceive.get())));

		sendCount.set(0);
		receiveCount.set(0);
	}

	// @Test
	public void suddenDownTest() throws Exception {
		startBroker();
		System.in.read();
	}

	// @Test
	public void produceOne() throws IOException {
		Producer p = lookup(Producer.class);

		p.message(TOPIC, null, sendCount.get()).send();
		System.in.read();
	}

	// @Test
	public void consumeOne() throws IOException {
		startConsumeThread();
		System.in.read();
	}

	@Test
	public void myTest() throws Exception {
		startBroker();
		startCountTimer();
		startProduceThread();
		startConsumeThread();

		System.in.read();
	}

	private void startBroker() throws Exception {
		new Thread(new Runnable() {
			@Override
			public void run() {

				try {
					lookup(BrokerBootstrap.class).start();
				} catch (Exception e) {
					System.out.println("Fail to start Broker: " + e.getMessage());
				}
			}
		}).start();

		TimeUnit.SECONDS.sleep(2);
	}

	private void startCountTimer() {
		new Timer().schedule(new TimerTask() {
			@Override
			public void run() {
				printAndClean();
			}
		}, 1000, timeInterval);
	}

	private void startProduceThread() {
		for (int i = 0; i < threadNum; i++) {
			runProducer();
		}
	}

	private void runProducer() {
		new Thread(new Runnable() {
			@Override
			public void run() {
				Producer p = lookup(Producer.class);

				for (;;) {
					SettableFuture<SendResult> future = (SettableFuture<SendResult>) p.message(TOPIC, null, sendCount.get())
					      .send();

					Futures.addCallback(future, new FutureCallback<SendResult>() {
						@Override
						public void onSuccess(SendResult result) {
							sendCount.addAndGet(1);
						}

						@Override
						public void onFailure(Throwable t) {
							sendCount.addAndGet(1);
						}
					});

					// try {
					// Thread.sleep(1);
					// } catch (InterruptedException e) {
					// e.printStackTrace();
					// }
				}
			}
		}).start();
	}

	private void startConsumeThread() {
		new Thread(new Runnable() {
			@Override
			public void run() {
				Engine engine = lookup(Engine.class);

				Subscriber s = new Subscriber(TOPIC, "group1", new MessageListener<String>() {
					@Override
					public void onMessage(List<ConsumerMessage<String>> msgs) {
						receiveCount.addAndGet(msgs.size());
						for (ConsumerMessage<?> msg : msgs) {
							msg.ack();
						}
					}
				});

				engine.start(s);
			}
		}).start();
	}

	final static String stangeString = "{\"1\":{\"str\":\"429bb071-7d14-4da7-9ef1-a6f5b17911b5\"},"
	      + "\"2\":{\"str\":\"ExchangeTest\"},\"3\":33333{\"i32\":8},\"4\":{\"str\":\"uft-8\"},"
	      + "\"5\":{\"str\":\"cmessage-adapter 1.0\"},\"6\":{\"i32\":3},\"7\":{\"i32\":1},\"8\":{\"i32\":0},\"9\":{\"str\":\"order_new\"},\"10\":{\"str\":\"\"},\"11\":{\"str\":\"1\"},\"12\":{\"str\":\"DST56615\"},\"13\":{\"str\":\"555555\"},\"14\":{\"str\":\"169.254.142.159\"},\"15\":{\"str\":\"java.lang.String\"},\"16\":{\"i64\":1429168996889},\"17\":{\"map\":[\"str\",\"str\",0,{}]}}";

	// @Test
	public void integratedTest() throws Exception {
		startBroker();
		startConsumeThreadStrange();

		Producer p = lookup(Producer.class);

		p.message(TOPIC, null, "some content").addProperty("strangeString", stangeString).send();

		System.in.read();
	}

	private void startConsumeThreadStrange() {
		new Thread(new Runnable() {
			@Override
			public void run() {
				String topic = TOPIC;
				Engine engine = lookup(Engine.class);

				Subscriber s = new Subscriber(topic, "group1", new MessageListener<String>() {
					@Override
					public void onMessage(List<ConsumerMessage<String>> msgs) {
						for (ConsumerMessage<String> msg : msgs) {
							Iterator<String> it = msg.getPropertyNames();
							System.out.println("msg: " + msg.getBody());
							while (it.hasNext()) {
								String key = it.next();
								System.out.println("key: " + key + ", value: " + msg.getProperty(key));
							}

						}
					}
				});

				engine.start(s);
			}
		}).start();
	}
}
