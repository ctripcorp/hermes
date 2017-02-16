package com.ctrip.hermes.producer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.producer.api.Producer;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class StartProducer extends ComponentTestCase {
	private ExecutorService m_executors = Executors.newFixedThreadPool(5);

	@Test
	public void test() throws Exception {
		System.out.println("Producer started...");

		// VIServer viServer = new VIServer(8080);
		// viServer.start();

		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		while (true) {
			String line = in.readLine();
			if ("quit".equals(line)) {
				break;
			} else {
				String topic = "order_new";
				if (!StringUtils.isBlank(line)) {
					topic = StringUtils.trim(line);
				}
				send(topic);
			}

		}
	}

	private void send(String topic) throws Exception {
		Random random = new Random();
		String uuid = UUID.randomUUID().toString();

		boolean priority = random.nextBoolean();
		final String msg = String.format("%s %s %s", topic, uuid, priority ? "priority" : "non-priority");
		System.out.println(String.format("Sending message to topic %s(body: %s)", topic, msg));
		SettableFuture<SendResult> future = null;
		if (priority) {
			future = (SettableFuture<SendResult>) Producer.getInstance()//
			      .message(topic, uuid, msg)//
			      .withRefKey(uuid)//
			      .withPriority()//
			      .send();
		} else {
			future = (SettableFuture<SendResult>) Producer.getInstance()//
			      .message(topic, uuid, msg)//
			      .withRefKey(uuid)//
			      .send();
		}

		Futures.addCallback(future, new FutureCallback<SendResult>() {

			@Override
			public void onSuccess(SendResult result) {
				System.out.println("Message " + msg + " sent.");
			}

			@Override
			public void onFailure(Throwable t) {
				System.out.println(String.format("Failed to send message %s.", msg));

			}
		}, m_executors);
	}
}
