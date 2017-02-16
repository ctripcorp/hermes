package com.ctrip.hermes.consumer.engine.bootstrap.strategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.consumer.engine.ConsumerContext;
import com.ctrip.hermes.consumer.engine.SubscribeHandle;
import com.ctrip.hermes.consumer.engine.config.ConsumerConfig;
import com.ctrip.hermes.core.utils.HermesThreadFactory;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public abstract class BaseConsumingStrategy implements ConsumingStrategy {

	private static final Logger log = LoggerFactory.getLogger(BaseConsumingStrategy.class);

	@Inject
	protected ConsumerConfig m_config;

	@Override
	public SubscribeHandle start(ConsumerContext context, int partitionId) {

		try {
			int localCacheSize = m_config.getLocalCacheSize(context.getTopic().getName());
			int maxAckHolderSize = m_config.getMaxAckHolderSize(context.getTopic().getName());

			int notifierThreadCount = m_config.getNotifierThreadCount(context.getTopic().getName());
			int notifierWorkQueueSize = m_config.getNotifierWorkQueueSize(context.getTopic().getName());
			int minDeliveredMsgCount = (notifierWorkQueueSize + notifierThreadCount + 1) * localCacheSize;

			if (maxAckHolderSize < minDeliveredMsgCount) {
				log.warn(
				      "Bad maxAckHolderSize({}), will use {} as maxAckHolderSize(notifierThreadCount={}, localCacheSize={}).",
				      maxAckHolderSize, minDeliveredMsgCount, notifierThreadCount, localCacheSize);
				maxAckHolderSize = minDeliveredMsgCount;
			}

			final ConsumerTask consumerTask = getConsumerTask(context, partitionId, localCacheSize, maxAckHolderSize);

			Thread thread = HermesThreadFactory
			      .create(
			            String.format("ConsumerThread-%s-%s-%s", context.getTopic().getName(), partitionId,
			                  context.getGroupId()), false).newThread(new Runnable() {

				      @Override
				      public void run() {
					      consumerTask.start();
				      }
			      });

			thread.start();

			SubscribeHandle subscribeHandle = new SubscribeHandle() {

				@Override
				public void close() {
					consumerTask.close();
				}
			};

			return subscribeHandle;
		} catch (Exception e) {
			throw new RuntimeException(String.format("Failed to start consumer(topic=%s, partition=%s, groupId=%s).",
			      context.getTopic().getName(), partitionId, context.getGroupId()), e);
		}
	}

	protected abstract ConsumerTask getConsumerTask(ConsumerContext context, int partitionId, int localCacheSize,
	      int maxAckHolderSize);

}
