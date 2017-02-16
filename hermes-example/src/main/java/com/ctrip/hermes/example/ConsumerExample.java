package com.ctrip.hermes.example;

import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.consumer.api.BaseMessageListener;
import com.ctrip.hermes.consumer.engine.Engine;
import com.ctrip.hermes.consumer.engine.Subscriber;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.example.common.Configuration;

public class ConsumerExample {

	private static Logger logger = LoggerFactory.getLogger(ConsumerExample.class);

	private static String groupId = null;

	private static String topic = null;

	public static void main(String[] args) {
		init();
		runConsumer();
	}

	private static void init() {
		Configuration.addResource("hermes.properties");
		topic = Configuration.get("consumer.topic", "cmessage_fws");
		groupId = Configuration.get("consumer.groupid", "group1");
	}

	private static void runConsumer() {
		logger.info(String.format("Consumer Example Started.\nTopic: %s, GroupId: %s", topic, groupId));

		final AtomicInteger i = new AtomicInteger(0);
		Engine engine = PlexusComponentLocator.lookup(Engine.class);

		Subscriber s = new Subscriber(topic, groupId, new BaseMessageListener<String>() {

			@Override
			protected void onMessage(ConsumerMessage<String> msg) {
				// logger.info("==== ConsumedMessage ==== \n" + msg.toString());

				if (i.incrementAndGet() % 1000 == 0)
					logger.info("ConsumerReceived count: " + i.get());
			}
		});
		engine.start(s);
	}
}
