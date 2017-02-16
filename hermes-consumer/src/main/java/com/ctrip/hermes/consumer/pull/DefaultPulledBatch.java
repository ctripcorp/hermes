package com.ctrip.hermes.consumer.pull;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import com.ctrip.hermes.consumer.api.OffsetCommitCallback;
import com.ctrip.hermes.consumer.api.PulledBatch;
import com.ctrip.hermes.core.constants.CatConstants;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.utils.CatUtil;
import com.dianping.cat.message.Transaction;
import com.google.common.util.concurrent.SettableFuture;

public class DefaultPulledBatch<T> implements PulledBatch<T> {

	private List<ConsumerMessage<T>> m_messages;

	private RetriveSnapshot<T> m_snapshot;

	private ExecutorService m_callbackExecutor;

	private String m_topic;

	private String m_group;

	private long m_bornTime;

	public DefaultPulledBatch(String topic, String group, List<ConsumerMessage<T>> msgs, RetriveSnapshot<T> snapshot,
	      ExecutorService callbackExecutor) {
		m_topic = topic;
		m_group = group;
		m_messages = msgs;
		m_snapshot = snapshot;
		m_callbackExecutor = callbackExecutor;
		m_bornTime = System.currentTimeMillis();
	}

	@Override
	public List<ConsumerMessage<T>> getMessages() {
		return m_messages;
	}

	@SuppressWarnings("unchecked")
	@Override
	public synchronized void commitAsync(OffsetCommitCallback callback) {
		if (m_snapshot.isDone()) {
			callback.onComplete(Collections.EMPTY_MAP, null);
		} else {
			m_snapshot.commitAsync(callback, m_callbackExecutor);
			logProcessTime();
		}
	}

	private void logProcessTime() {
		CatUtil.logElapse(CatConstants.TYPE_MESSAGE_CONSUMED, m_topic + ":" + m_group, m_bornTime, m_messages.size(),
		      null, Transaction.SUCCESS);
	}

	@Override
	public synchronized void commitAsync() {
		commitAsync(null);
	}

	@Override
	public synchronized void commitSync() {
		if (!m_snapshot.isDone()) {
			SettableFuture<Boolean> future = m_snapshot.commitAsync(null, m_callbackExecutor);
			logProcessTime();
			try {
				future.get();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			} catch (ExecutionException e) {
				// won't happen
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public long getBornTime() {
		return m_bornTime;
	}

}
