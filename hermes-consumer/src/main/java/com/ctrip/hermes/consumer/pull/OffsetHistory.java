package com.ctrip.hermes.consumer.pull;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.ctrip.hermes.core.message.ConsumerMessage;

/**
 * Offset history of a "table"(priority, non-priority, re-send)
 *
 */
public class OffsetHistory<T> {
	private OffsetAndNackMessages<T> m_lastRetriveResult;

	private OffsetAndNackMessages<T> m_lastLastRetriveResult;

	private AtomicLong m_committedOffset = new AtomicLong(0L);

	public OffsetHistory(long lastRetriveOffset) {
		m_lastRetriveResult = new OffsetAndNackMessages<T>(lastRetriveOffset);
		m_lastLastRetriveResult = new OffsetAndNackMessages<T>(0L);
	}

	public void forward() {
		if (m_lastRetriveResult.getOffset() > m_lastLastRetriveResult.getOffset()) {
			m_lastLastRetriveResult.setOffset(m_lastRetriveResult.getOffset());
		}

		if (!m_lastRetriveResult.getNackMessages().isEmpty()) {
			m_lastRetriveResult.getNackMessages().drainTo(m_lastLastRetriveResult.getNackMessages());
		}
	}

	public void setCommittedOffset(long committedOffset) {
		m_committedOffset.set(committedOffset);
	}

	public long getCommittedOffset() {
		return m_committedOffset.get();
	}

	public OffsetAndNackMessages<T> getLastRetriveResult() {
		return m_lastRetriveResult;
	}

	public OffsetAndNackMessages<T> getLastLastRetriveResult() {
		return m_lastLastRetriveResult;
	}

	public static class OffsetAndNackMessages<T> {
		private AtomicLong m_offset = new AtomicLong();

		private BlockingQueue<ConsumerMessage<T>> nackMessages;

		public OffsetAndNackMessages(long offset) {
			m_offset.set(offset);
			nackMessages = new LinkedBlockingQueue<>();
		}

		public long getOffset() {
			return m_offset.get();
		}

		public BlockingQueue<ConsumerMessage<T>> getNackMessages() {
			return nackMessages;
		}

		public void setOffset(long offset) {
			m_offset.set(offset);
		}

		public void addNackMessage(ConsumerMessage<T> msg) {
			nackMessages.add(msg);
		}

		@Override
		public String toString() {
			return "OffsetAndNackMessages [m_offset=" + m_offset + ", nackMessages=" + nackMessages + "]";
		}

	}

	@Override
	public String toString() {
		return "OffsetHistory [m_lastRetriveResult=" + m_lastRetriveResult + ", m_lastLastRetriveResult="
		      + m_lastLastRetriveResult + ", m_committedOffset=" + m_committedOffset + "]";
	}

}
