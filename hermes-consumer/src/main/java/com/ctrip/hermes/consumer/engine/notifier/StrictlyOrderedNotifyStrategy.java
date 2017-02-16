package com.ctrip.hermes.consumer.engine.notifier;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.consumer.engine.ConsumerContext;
import com.ctrip.hermes.consumer.message.BrokerConsumerMessage;
import com.ctrip.hermes.consumer.message.NackDelayedBrokerConsumerMessage;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.message.ConsumerMessage.MessageStatus;
import com.ctrip.hermes.core.message.retry.RetryPolicy;
import com.ctrip.hermes.core.pipeline.Pipeline;

public class StrictlyOrderedNotifyStrategy implements NotifyStrategy {

	private RetryPolicy m_retryPolicy;

	public StrictlyOrderedNotifyStrategy(RetryPolicy retryPolicy) {
		m_retryPolicy = retryPolicy;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void notify(List<ConsumerMessage<?>> msgs, ConsumerContext context, ExecutorService executorService,
	      Pipeline<Void> pipeline) {
		for (ConsumerMessage<?> msg : msgs) {
			NackDelayedBrokerConsumerMessage nackDelayedMsg = new NackDelayedBrokerConsumerMessage(
			      (BrokerConsumerMessage<?>) msg);
			List singleMsg = Arrays.asList(nackDelayedMsg);

			int retries = 0;
			while (!Thread.interrupted()) {
				pipeline.put(new Pair<ConsumerContext, List<?>>(context, singleMsg));

				if (nackDelayedMsg.getBaseConsumerMessage().getStatus() == MessageStatus.FAIL) {
					// reset status to enable reconsume or nack
					nackDelayedMsg.getBaseConsumerMessage().resetStatus();
					if (retries < m_retryPolicy.getRetryTimes()) {
						sleep(m_retryPolicy.nextScheduleTimeMillis(retries, 0));
						nackDelayedMsg.setResend(true);
						nackDelayedMsg.setRemainingRetries(m_retryPolicy.getRetryTimes() - retries);
						retries++;
					} else {
						msg.nack();
						break;
					}
				} else {
					msg.ack();
					break;
				}
			}
		}
	}

	private void sleep(long timeMills) {
		try {
			Thread.sleep(timeMills);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

}
