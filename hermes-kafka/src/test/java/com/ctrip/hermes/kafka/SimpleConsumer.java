package com.ctrip.hermes.kafka;

import java.io.IOException;

import com.ctrip.hermes.consumer.api.BaseMessageListener;
import com.ctrip.hermes.consumer.api.Consumer;
import com.ctrip.hermes.core.message.ConsumerMessage;

public class SimpleConsumer {
	public static void main(String[] args) throws IOException {
		Consumer.getInstance().start("yqtest", "yqtest", new BaseMessageListener<String>() {

			@Override
			protected void onMessage(ConsumerMessage<String> msg) {
				System.out.println("Received message " + msg.getOffset());
				try {
					Thread.sleep(1000L);
				} catch (InterruptedException e) {
				}
			}
		});

		System.in.read();
	}
}
