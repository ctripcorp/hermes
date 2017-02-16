package com.ctrip.hermes.example;

import java.util.UUID;

import com.ctrip.hermes.consumer.api.BaseMessageListener;
import com.ctrip.hermes.consumer.api.Consumer;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.result.CompletionCallback;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.producer.api.Producer;

public class ZkSwitchProducerConsumer {
	public static void main(String[] args) {
		String topic = "yq.test2";
		String consumer = "yq.test";
		Consumer.getInstance().start(topic, consumer, new BaseMessageListener<String>() {

			@Override
			protected void onMessage(ConsumerMessage<String> msg) {
				System.out.println("RECEIVED MESSAGE: " + msg.getBody());
			}
		});

		while (true) {
			String uuid = UUID.randomUUID().toString();
			final String msg = String.format("%s %s ", topic, uuid);
			Producer.getInstance().message(topic, uuid, msg).setCallback(new CompletionCallback<SendResult>() {

				@Override
				public void onSuccess(SendResult result) {
					System.out.println("Send msg:" + msg + " success.");
				}

				@Override
				public void onFailure(Throwable t) {
					System.out.println(String.format("Send message " + msg + " failed"));

				}
			}).send();
			try {
	         Thread.sleep(1000);
         } catch (InterruptedException e) {
	         // TODO Auto-generated catch block
	         e.printStackTrace();
         }
		}
	}
}
