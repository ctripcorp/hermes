package com.ctrip.hermes.producer;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.core.result.CompletionCallback;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.producer.api.Producer;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class ProducerStressTest extends ComponentTestCase {

	public static void main(String[] args) throws Exception {
		// String topic = "hello_world";
		String topic = "order_new";
		int bodyLength = 100;
		int paritionCount = 1;
		int msgPerSeconds = 600;

		System.in.read();

		System.out.println("Producer started...");

		AtomicLong sending = new AtomicLong(0);
		final AtomicLong sent = new AtomicLong(0);
		final AtomicLong failed = new AtomicLong(0);

		StatTask statTask = new StatTask(sending, sent, failed, 5 * 1000);

		Thread statThread = new Thread(statTask);
		statThread.start();

		ProducerTask[] producerTasks = new ProducerTask[paritionCount];
		for (int i = 0; i < paritionCount; i++) {
			producerTasks[i] = new ProducerTask(i, sending, sent, failed, topic, generateRandomString(bodyLength),
			      msgPerSeconds / paritionCount);
			Thread producerThread = new Thread(producerTasks[i]);
			producerThread.start();
		}

		System.in.read();
		for (ProducerTask producerTask : producerTasks) {
			producerTask.stop();
		}

		System.out.println("Producer stopped...");

		System.in.read();
		statTask.stop();
		System.out.println("Stat stopped...");

		System.in.read();

	}

	private static class StatTask implements Runnable {

		private AtomicLong sending;

		private AtomicLong sent;

		private AtomicLong failed;

		private long interval;

		private AtomicBoolean stopped = new AtomicBoolean(false);

		public StatTask(AtomicLong sending, AtomicLong sent, AtomicLong failed, long interval) {
			this.sending = sending;
			this.sent = sent;
			this.failed = failed;
			this.interval = interval;
		}

		@Override
		public void run() {
			while (!stopped.get()) {
				System.out.println(String.format("Sending: %s, Sent: %s, Failed: %s", sending.get(), sent.get(),
				      failed.get()));

				try {
					Thread.sleep(interval);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

			}
		}

		public void stop() {
			stopped.set(true);
		}

	}

	private static class ProducerTask implements Runnable {
		private AtomicLong sending;

		private AtomicLong sent;

		private AtomicLong failed;

		private AtomicBoolean stopped = new AtomicBoolean(false);

		private String topic;

		private String body;

		private int msgsPerSec;

		private int num;

		public ProducerTask(int num, AtomicLong sending, AtomicLong sent, AtomicLong failed, String topic, String body,
		      int msgsPerSec) {
			this.sending = sending;
			this.sent = sent;
			this.failed = failed;
			this.body = body;
			this.topic = topic;
			this.msgsPerSec = msgsPerSec;
			this.num = num;
		}

		@Override
		public void run() {
			long id = 0;
			while (!stopped.get()) {
				sending.incrementAndGet();
				id++;
				Producer.getInstance().message(topic, String.valueOf(num), id + "--" + body).withRefKey(Long.toString(id))
				      .setCallback(new CompletionCallback<SendResult>() {

					      @Override
					      public void onSuccess(SendResult result) {
						      sent.incrementAndGet();
					      }

					      @Override
					      public void onFailure(Throwable t) {
						      failed.incrementAndGet();
					      }
				      }).send();

				if (id % msgsPerSec == 0) {
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}

		public void stop() {
			stopped.set(true);
		}

	}

	private static String generateRandomString(int size) {
		StringBuilder sb = new StringBuilder(size);
		Random random = new Random();
		for (int i = 0; i < size; i++) {
			sb.append('a' + random.nextInt(25));
		}

		return sb.toString();
	}

}
