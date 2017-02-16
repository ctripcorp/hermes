package com.ctrip.hermes.consumer.engine.bootstrap.strategy;

import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.consumer.engine.ConsumerContext;
import com.ctrip.hermes.consumer.engine.ack.AckManager;
import com.ctrip.hermes.consumer.engine.lease.ConsumerLeaseKey;
import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.schedule.ExponentialSchedulePolicy;
import com.ctrip.hermes.core.schedule.SchedulePolicy;
import com.ctrip.hermes.core.transport.command.v5.AckMessageCommandV5;

public class PullConsumingStrategyConsumerTask extends BaseConsumerTask {
	private static final Logger log = LoggerFactory.getLogger(PullConsumingStrategyConsumerTask.class);

	public PullConsumingStrategyConsumerTask(ConsumerContext context, int partitionId, int localCacheSize,
	      int maxAckHolderSize) {
		super(context, partitionId, localCacheSize, DummyAckManager.INSTANCE);
	}

	@Override
	protected void queryLatestOffset(ConsumerLeaseKey key) {
		if (m_context.getOffsetStorage() != null) {
			SchedulePolicy queryOffsetSchedulePolicy = new ExponentialSchedulePolicy(
			      (int) m_config.getQueryOffsetTimeoutMillis(), (int) m_config.getQueryOffsetTimeoutMillis());

			while (!isClosed() && !Thread.currentThread().isInterrupted() && !m_lease.get().isExpired()) {
				try {
					String topic = m_context.getTopic().getName();
					Offset offset = m_context.getOffsetStorage().queryLatestOffset(topic, m_partitionId);
					if (offset == null) {
						queryOffsetSchedulePolicy.fail(true);
						continue;
					}
					m_offset.set(new AtomicReference<Offset>(new Offset(offset.getPriorityOffset(), offset
					      .getNonPriorityOffset(), null)));
					return;
				} catch (Exception e) {
					log.error("Query latest offset failed: {}:{}", m_context.getTopic().getName(), m_partitionId, e);
					queryOffsetSchedulePolicy.fail(true);
				}
			}
		} else {
			super.queryLatestOffset(key);
		}
	}

	private static class DummyAckManager implements AckManager {

		public static final AckManager INSTANCE = new DummyAckManager();

		private DummyAckManager() {
		}

		@Override
		public void register(long token, Tpg tpg, int maxAckHolderSize) {
		}

		@Override
		public void ack(long token, ConsumerMessage<?> msg) {
		}

		@Override
		public void nack(long token, ConsumerMessage<?> msg) {
		}

		@Override
		public void delivered(long token, ConsumerMessage<?> msg) {
		}

		@Override
		public void deregister(long token) {
		}

		@Override
		public boolean writeAckToBroker(AckMessageCommandV5 cmd) {
			return true;
		}

	}
}
