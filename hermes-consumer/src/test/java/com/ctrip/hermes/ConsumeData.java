package com.ctrip.hermes;

import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.consumer.api.BaseMessageListener;
import com.ctrip.hermes.consumer.api.Consumer;
import com.ctrip.hermes.consumer.api.MessageListenerConfig;
import com.ctrip.hermes.consumer.api.MessageListenerConfig.StrictlyOrderingRetryPolicy;
import com.ctrip.hermes.core.message.ConsumerMessage;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class ConsumeData {
	private static final Logger log = LoggerFactory.getLogger(ConsumeData.class);

	@Test
	public void test() throws Exception {
		final CountDownLatch latch = new CountDownLatch(100000);
		MessageListenerConfig config = new MessageListenerConfig();
		config.setStrictlyOrderingRetryPolicy(StrictlyOrderingRetryPolicy.evenRetry(10, 2));

		Consumer.getInstance().start("lpt_test", "leo_test", new BaseMessageListener<String>() {

			@Override
			protected void onMessage(ConsumerMessage<String> msg) {
				log.error(msg.getBody());
				Random random = new Random();

				if (msg.isResend()) {
					if (msg.getRemainingRetries() > 1) {
						if (random.nextInt(9) < 3) {
							msg.nack();
						} else {
							msg.ack();
							latch.countDown();
						}
					} else {
						msg.ack();
						latch.countDown();
					}
				} else {
					if (random.nextInt(9) < 3) {
						msg.nack();
					} else {
						msg.ack();
						latch.countDown();
					}
				}
			}
		});

		latch.await();

		System.out.println("Done..");
		System.in.read();
	}
}
