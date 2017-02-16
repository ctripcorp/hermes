package com.ctrip.hermes.broker.ack.internal;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.utils.CollectionUtil;

public class DefaultAckHolder<T> implements AckHolder<T> {

	private final static Logger log = LoggerFactory.getLogger(DefaultAckHolder.class);

	private List<Batch> m_batches = new LinkedList<>();

	private int m_timeout;

	public DefaultAckHolder(int timeout) {
		m_timeout = timeout;
	}

	@Override
	public BatchResult<T> scan() {
		BatchResult<T> result = null;

		Iterator<Batch> it = m_batches.iterator();
		while (it.hasNext()) {
			Batch batch = it.next();
			if (batch.isTimeout(m_timeout) || batch.allAcked()) {
				it.remove();
				if (result == null) {
					result = batch.getResult();
				} else {
					result.merge(batch.getResult());
				}
			} else {
				break;
			}
		}

		return result;
	}

	@Override
	public void delivered(List<Pair<Long, T>> offsets, long develiveredTime) {
		EnumRange<T> range = new EnumRange<>(offsets);

		m_batches.add(new Batch(range, develiveredTime));
	}

	@Override
	public void acked(long offset, boolean success) {
		Batch batch = findBatch(offset);

		if (batch == null) {
			// maybe do nothing, since DefaultAckManager can guarantee ack/nack happen after deliver.
			// If reach here, that must be the batch has been timeout in the last scan.
		} else {
			batch.updateState(offset, success);
		}
	}

	private Batch findBatch(long offset) {
		for (Batch batch : m_batches) {
			if (batch.contains(offset)) {
				return batch;
			}
		}
		return null;
	}

	protected boolean isTimeout(long start, int timeout) {
		return System.currentTimeMillis() > start + timeout;
	}

	private enum State {
		INIT, SUCCESS, FAIL
	}

	private class Batch {

		private ContinuousRange m_continuousRange;

		private TreeMap<Long, State> m_map;

		private TreeMap<Long, T> m_ctxMap;

		private long m_ts;

		private int m_doneCount = 0;

		public Batch(EnumRange<T> range, long ts) {
			List<Pair<Long, T>> rangeOffsets = range.getOffsets();
			m_continuousRange = new ContinuousRange(CollectionUtil.first(rangeOffsets).getKey(), CollectionUtil.last(
			      rangeOffsets).getKey());

			m_ts = ts;

			m_map = new TreeMap<>();
			m_ctxMap = new TreeMap<>();
			for (Pair<Long, T> pair : rangeOffsets) {
				m_map.put(pair.getKey(), State.INIT);
				m_ctxMap.put(pair.getKey(), pair.getValue());
			}
		}

		public BatchResult<T> getResult() {
			return new BatchResult<>(getFailRange(), getDoneRange());
		}

		public ContinuousRange getDoneRange() {
			return m_continuousRange;
		}

		public EnumRange<T> getFailRange() {
			EnumRange<T> failRange = new EnumRange<>();

			for (Map.Entry<Long, State> entry : m_map.entrySet()) {
				if (entry.getValue() != State.SUCCESS) {
					long offset = entry.getKey();
					failRange.addOffset(offset, m_ctxMap.get(offset));
				}
				if (entry.getValue() == State.INIT) {
					log.warn("message {} didn't receive ack or nack before timeout, treat as nack", entry.getKey());
				}
				if (entry.getValue() == State.FAIL) {
					log.warn("message {} received nack ", entry.getKey());
				}
			}

			if (failRange.getOffsets().isEmpty()) {
				return null;
			} else {
				return failRange;
			}
		}

		public void updateState(long offset, boolean success) {
			State oldState = m_map.put(offset, success ? State.SUCCESS : State.FAIL);
			if (oldState == State.INIT) {
				m_doneCount++;
			}
		}

		public boolean allAcked() {
			return m_doneCount == m_map.size();
		}

		public boolean isTimeout(int timeout) {
			return DefaultAckHolder.this.isTimeout(m_ts, timeout);
		}

		public boolean contains(long offset) {
			return m_map.containsKey(offset);
		}

	}

	@Override
	public long getMaxAckedOffset() {
		throw new UnsupportedOperationException("Can not dectect DefaultAckHolder's max offset.");
	}

}
