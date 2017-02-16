package com.ctrip.hermes.broker.ack.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.message.TppConsumerMessageBatch.MessageMeta;

public class ForwardOnlyAckHolder implements AckHolder<MessageMeta> {
	private long m_maxAckedOffset = -1;

	private boolean m_modified = false;

	private BlockingQueue<Pair<Long, MessageMeta>> m_deliveredMessageMetas = new LinkedBlockingQueue<>();

	private BlockingQueue<Pair<Long, MessageMeta>> m_nackedMessageMetas = new LinkedBlockingQueue<>();

	@Override
	public void delivered(List<Pair<Long, MessageMeta>> range, long develiveredTime) {
		for (Pair<Long, MessageMeta> pair : range) {
			m_deliveredMessageMetas.offer(pair);
		}
	}

	@Override
	public void acked(long offset, boolean success) {
		Pair<Long, MessageMeta> messageMetaOfOffset = clearDeliveredMessageMetasBefore(offset);

		long oldValue = m_maxAckedOffset;
		m_maxAckedOffset = Math.max(offset, m_maxAckedOffset);
		m_modified = m_modified || m_maxAckedOffset > oldValue;

		if (!success) {
			if (messageMetaOfOffset != null) {
				m_nackedMessageMetas.offer(messageMetaOfOffset);
				m_modified = true;
			}
		}
	}

	private Pair<Long, MessageMeta> clearDeliveredMessageMetasBefore(long offset) {
		Pair<Long, MessageMeta> messageMetaOfOffset = null;

		while (!m_deliveredMessageMetas.isEmpty()) {
			Pair<Long, MessageMeta> cur = m_deliveredMessageMetas.peek();
			if (cur != null) {
				if (cur.getKey() < offset) {
					m_deliveredMessageMetas.poll();
					continue;
				} else if (cur.getKey() == offset) {
					messageMetaOfOffset = m_deliveredMessageMetas.poll();
					break;
				} else {
					// cur.getId() > offset
					break;
				}
			} else {
				break;
			}
		}

		return messageMetaOfOffset;
	}

	@Override
	public BatchResult<MessageMeta> scan() {
		if (m_modified) {
			m_modified = false;

			EnumRange<MessageMeta> failRange = null;
			if (!m_nackedMessageMetas.isEmpty()) {
				List<Pair<Long, MessageMeta>> pendingNacks = new ArrayList<>(m_nackedMessageMetas.size());
				m_nackedMessageMetas.drainTo(pendingNacks);
				failRange = new EnumRange<>(pendingNacks);
			}

			// TODO skip done range when m_maxAckedOffset not modified
			return new BatchResult<MessageMeta>(failRange, new ContinuousRange(-1, m_maxAckedOffset));
		}
		return null;
	}

	@Override
	public long getMaxAckedOffset() {
		return m_maxAckedOffset;
	}
}
