package com.ctrip.hermes.kafka;

import com.ctrip.hermes.producer.api.Producer;

public class SimpleProducer {
	public static void main(String[] args) throws InterruptedException {
		while (true) {
			Producer.getInstance().message("yqtest", "pk", "123").send();
			Thread.sleep(500L);
		}

	}
}
