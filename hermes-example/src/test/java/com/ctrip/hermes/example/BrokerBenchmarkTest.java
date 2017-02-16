package com.ctrip.hermes.example;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.ctrip.hermes.consumer.api.BaseMessageListener;
import com.ctrip.hermes.consumer.api.Consumer;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.producer.api.Producer;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class BrokerBenchmarkTest {

	public static String randomString(int len) {
		String chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
		Random rand = new Random();
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < len; i++) {
			sb.append(chars.charAt(rand.nextInt(chars.length())));
		}
		return sb.toString();
	}

	public static void main(String[] args) throws Exception {
		final String topic = "order_new";
		final String[] msgs = new String[] { randomString(1 * 1000), randomString(10 * 1000), randomString(100 * 1000),
		      randomString(300 * 1000) };
		Consumer.getInstance().start(topic, "leo1", new BaseMessageListener<String>() {

			@Override
         protected void onMessage(ConsumerMessage<String> msg) {
	         System.out.println("Message Received...");
         }
		});

		int concurrent = 3;
		
		final CountDownLatch start = new CountDownLatch(1);
		for (int i = 0; i < concurrent; i++) {
			new Thread(new Runnable() {

				@Override
				public void run() {
					try {
						start.await();
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					Random random = new Random();
					while (true) {
						Producer.getInstance()
						      .message(topic, UUID.randomUUID().toString(), msgs[random.nextInt(msgs.length)]).send();
						try {
							TimeUnit.MILLISECONDS.sleep(10);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			}).start();
			;
		}
		System.out.println("Press any key to start");
		System.in.read();
		start.countDown();
		System.out.println("Started...");
		System.in.read();
	}
}
