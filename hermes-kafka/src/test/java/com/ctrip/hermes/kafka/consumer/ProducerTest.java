package com.ctrip.hermes.kafka.consumer;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.producer.api.Producer;
import com.ctrip.hermes.producer.api.Producer.MessageHolder;

public class ProducerTest {

	@Test
	public void testSimpleProducer() throws InterruptedException, ExecutionException {
		String topic = "kafka.SimpleTextTopic";

		Producer producer = Producer.getInstance();

		List<Future<SendResult>> result = new ArrayList<Future<SendResult>>();
		for (int i = 0; i < 2; i++) {
			VisitEvent event = generateEvent();
			MessageHolder holder = producer.message(topic, null, event);
			Future<SendResult> send = holder.send();
			result.add(send);
		}

		for (Future<SendResult> future : result) {
			future.get();
			if (future.isDone()) {
				System.out.println("DONE: " + future.toString());
			}
		}
	}

	static AtomicLong counter = new AtomicLong();

	static VisitEvent generateEvent() {
		Random random = new Random(System.currentTimeMillis());
		VisitEvent event = new VisitEvent();
		event.ip = "192.168.0." + random.nextInt(255);
		event.tz = new Date();
		event.url = "www.ctrip.com/" + counter.incrementAndGet();
		return event;
	}
}
