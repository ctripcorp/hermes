package com.ctrip.hermes.example.feature.producer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.broker.bootstrap.BrokerBootstrap;
import com.ctrip.hermes.consumer.api.MessageListener;
import com.ctrip.hermes.consumer.engine.Engine;
import com.ctrip.hermes.consumer.engine.Subscriber;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.producer.api.Producer;

public class OneProducer extends ComponentTestCase {

	static final String TOPIC = "order.new";

	public void startBroker() throws Exception {
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					lookup(BrokerBootstrap.class).start();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}).start();
	}

	@Test
	public void SendOneMsg() throws Exception {
		startBroker();

		final String msg = "Feature Test: SendOneMsg()";
		final CountDownLatch latch = new CountDownLatch(1);

		Producer p = lookup(Producer.class);
		p.message(TOPIC, null, msg).send();

		Engine engine = lookup(Engine.class);
		Subscriber s = new Subscriber(TOPIC, "group1", new MessageListener<String>() {
			@Override
			public void onMessage(List<ConsumerMessage<String>> msgs) {
				assertEquals(msgs.size(), 1);
				assertEquals(msgs.get(0).getBody(), msg);
				latch.countDown();
			}
		});

		engine.start(s);

		assertTrue(latch.await(1, TimeUnit.SECONDS));
	}

	@Test
	public void SendManyMsg() throws Exception {
		startBroker();
		// todo
	}

	/**
	 * 发不同优先级的msgs: 1, 对于已经存在队列中的，高优先级先收到； 2, 新来的高优先级，优先被发送。 //不易在client端实现，应在server端test.
	 */
	@Test
	public void HighPriorityFirstReceived() throws ComponentLookupException, InterruptedException {
		// OldProducer oldProducer = buildSyncProducer(factory);
		//
		//
		// final List<Message> receivedMsgs = new ArrayList<>();
		// final CountDownLatch latch = new CountDownLatch(3 * msgBatchCount);
		// OldConsumer oldConsumer = buildConsumer(factory);
		// oldConsumer.setMessageListener(new MessageListener() {
		// @Override
		// public void onMessage(Message msg) {
		// System.out.println(msg.getPriority());
		// receivedMsgs.add(msg);
		// latch.countDown();
		// }
		// });
		// oldConsumer.start();
		// oldConsumer.stop();
		//
		// batchSendMsgs(oldProducer, msgBatchCount, MessagePriority.LOW);
		// batchSendMsgs(oldProducer, msgBatchCount, MessagePriority.MIDDLE);
		// batchSendMsgs(oldProducer, msgBatchCount, MessagePriority.HIGH);
		//
		// oldConsumer.start();
		// assertTrue(latch.await(5, TimeUnit.SECONDS));
		//
		// for (int i = 0; i < msgBatchCount; i++) {
		// assertEquals(receivedMsgs.get(i).getPriority(), MessagePriority.HIGH);
		// }
		// for (int i = msgBatchCount; i < 2 * msgBatchCount; i++) {
		// assertEquals(receivedMsgs.get(i).getPriority(), MessagePriority.MIDDLE);
		// }
		// for (int i = 2 * msgBatchCount; i < 3 * msgBatchCount; i++) {
		// assertEquals(receivedMsgs.get(i).getPriority(), MessagePriority.LOW);
		// }
	}

	/**
	 * 固定时间之后才能够收到的msg TODO: to be implemented.
	 */
	@Test
	public void produceDelayedMsgs() {
	}

}
