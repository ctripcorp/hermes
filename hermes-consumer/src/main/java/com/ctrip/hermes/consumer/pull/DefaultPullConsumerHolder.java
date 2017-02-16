package com.ctrip.hermes.consumer.pull;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.consumer.api.Consumer.ConsumerHolder;
import com.ctrip.hermes.consumer.api.MessageListener;
import com.ctrip.hermes.consumer.api.OffsetStorage;
import com.ctrip.hermes.consumer.api.PullConsumerConfig;
import com.ctrip.hermes.consumer.api.PullConsumerHolder;
import com.ctrip.hermes.consumer.api.PulledBatch;
import com.ctrip.hermes.consumer.engine.ack.AckManager;
import com.ctrip.hermes.consumer.engine.config.ConsumerConfig;
import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.constants.CatConstants;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.schedule.ExponentialSchedulePolicy;
import com.ctrip.hermes.core.schedule.SchedulePolicy;
import com.ctrip.hermes.core.utils.CatUtil;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;

public class DefaultPullConsumerHolder<T> implements PullConsumerHolder<T>, MessageListener<T> {

	private enum RetrivePolicy {
		FAVOUR_FAST_RETURN, FAVOUR_MORE_MESSAGE;
	}

	private final static Logger log = LoggerFactory.getLogger(DefaultPullConsumerHolder.class);

	private String m_topic;

	private PullConsumerConfig m_config;

	private ConsumerHolder m_consumerHolder;

	private int m_partitionCount;

	private List<BlockingQueue<PullBrokerConsumerMessage<T>>> m_partitionMsgs;

	private int m_scanStartIndex = 0;

	private ReentrantLock m_retriveLock = new ReentrantLock();

	private AtomicBoolean m_stopped = new AtomicBoolean(false);

	private Committer<T> m_committer;

	private ExecutorService m_callbackExecutor;

	private OffsetStorage m_offsetStorage;

	private String m_group;

	public DefaultPullConsumerHolder(String topic, String groupId, int partitionCount, PullConsumerConfig config,
	      AckManager ackManager, OffsetStorage offsetStorage, ConsumerConfig consumerConfig) {
		m_topic = topic;
		m_config = config;
		m_partitionCount = partitionCount;
		m_offsetStorage = offsetStorage;
		m_group = groupId;

		m_partitionMsgs = new ArrayList<>(partitionCount);
		for (int i = 0; i < partitionCount; i++) {
			m_partitionMsgs
			      .add(new ArrayBlockingQueue<PullBrokerConsumerMessage<T>>(config.getPartitionMessageCacheSize()));
		}

		m_committer = new DefaultCommitter<>(topic, groupId, partitionCount, config, ackManager, consumerConfig);

		String callbackName = "PullConsumerCallback-" + topic + "-" + groupId;
		m_callbackExecutor = Executors.newSingleThreadExecutor(HermesThreadFactory.create(callbackName, true));
	}

	@Override
	public void onMessage(List<ConsumerMessage<T>> msgs) {
		for (ConsumerMessage<T> msg : msgs) {
			try {
				if (msg.getPartition() >= m_partitionCount) {
					log.warn("Message partition {} is large than partition count {} of topic {}", msg.getPartition(),
					      m_partitionCount, m_topic);
				}

				m_partitionMsgs.get(msg.getPartition() % m_partitionCount).put((PullBrokerConsumerMessage<T>) msg);
				if (m_offsetStorage != null) {
					m_offsetStorage.updatePulledOffset(//
					      msg.getTopic(), //
					      msg.getPartition(), //
					      msg.isPriority() ? new Offset(msg.getOffset(), 0L, null) : new Offset(0L, msg.getOffset(), null)//
					      );
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				break;
			}
		}
	}

	@Override
	public PulledBatch<T> poll(int maxMessageCount, int timeout) {
		long startTime = System.currentTimeMillis();
		Transaction t = Cat.newTransaction(CatConstants.TYPE_MESSAGE_CONSUME_POLL_TRIED, m_topic + ":" + m_group);
		try {
			PulledBatch<T> batch = retrive(maxMessageCount, timeout, RetrivePolicy.FAVOUR_FAST_RETURN);
			if (batch.getMessages() != null && !batch.getMessages().isEmpty()) {
				CatUtil.logElapse(CatConstants.TYPE_MESSAGE_CONSUME_POLL_ELAPSE, m_topic + ":" + m_group, startTime, batch
				      .getMessages().size(), null, Transaction.SUCCESS);
			}
			return batch;
		} finally {
			t.setStatus(Transaction.SUCCESS);
			t.complete();
		}
	}

	@Override
	public PulledBatch<T> collect(int maxMessageCount, int timeout) {
		long startTime = System.currentTimeMillis();
		Transaction t = Cat.newTransaction(CatConstants.TYPE_MESSAGE_CONSUME_COLLECT_TRIED, m_topic + ":" + m_group);
		try {
			PulledBatch<T> batch = retrive(maxMessageCount, timeout, RetrivePolicy.FAVOUR_MORE_MESSAGE);
			if (batch.getMessages() != null && !batch.getMessages().isEmpty()) {
				CatUtil.logElapse(CatConstants.TYPE_MESSAGE_CONSUME_COLLECT_ELAPSE, m_topic + ":" + m_group, startTime,
				      batch.getMessages().size(), null, Transaction.SUCCESS);
			}
			return batch;
		} finally {
			t.setStatus(Transaction.SUCCESS);
			t.complete();
		}
	}

	@SuppressWarnings("unchecked")
	private PulledBatch<T> retrive(int maxMessageCount, int timeout, RetrivePolicy retrivePolicy) {
		long expireTime = System.currentTimeMillis() + timeout;
		LinkedList<ConsumerMessage<T>> msgs = new LinkedList<>();
		PulledBatch<T> result;

		boolean gotLock = false;
		try {
			gotLock = m_retriveLock.tryLock(timeout, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}

		if (gotLock) {
			try {
				SchedulePolicy retryPolicy = new ExponentialSchedulePolicy(m_config.getScanIntervalBase(),
				      m_config.getScanIntervalMax());
				while (System.currentTimeMillis() < expireTime) {
					int collected = collectCachedMessages(msgs, maxMessageCount);

					if (collected <= 0) {
						retryPolicy.fail(true);
					} else {
						if (retrivePolicy == RetrivePolicy.FAVOUR_FAST_RETURN) {
							break;
						} else if (retrivePolicy == RetrivePolicy.FAVOUR_MORE_MESSAGE) {
							if (msgs.size() >= maxMessageCount) {
								break;
							}
						}
					}
				}
			} catch (Exception e) {
				log.warn("Unexpected exception when collect cache messages", e);
			} finally {
				if (msgs.isEmpty()) {
					result = DummyPulledBatch.INSTANCE;
				} else {
					result = new DefaultPulledBatch<T>(m_topic, m_group, msgs, m_committer.delivered(msgs),
					      m_callbackExecutor);
				}
				m_retriveLock.unlock();
			}
		} else {
			result = DummyPulledBatch.INSTANCE;
		}

		return result;
	}

	private int collectCachedMessages(LinkedList<ConsumerMessage<T>> result, int targetSize) {
		int totalCollected = 0;

		for (int i = 0; i < m_partitionCount; i++) {
			if (result.size() >= targetSize) {
				break;
			} else {
				int index = (m_scanStartIndex + i) % m_partitionCount;
				totalCollected += m_partitionMsgs.get(index).drainTo(result, targetSize - result.size());
			}
		}

		m_scanStartIndex = (m_scanStartIndex + 1) % m_partitionCount;

		return totalCollected;
	}

	@Override
	public void close() {
		if (m_stopped.compareAndSet(false, true)) {
			if (m_consumerHolder != null) {
				m_consumerHolder.close();
			}
			m_committer.close();
			if (m_callbackExecutor != null) {
				m_callbackExecutor.shutdown();
			}
		}
	}

	public void setConsumerHolder(ConsumerHolder consumerHolder) {
		m_consumerHolder = consumerHolder;
	}

}
