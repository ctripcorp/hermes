package com.ctrip.hermes.broker.ack.internal;

import org.unidal.tuple.Pair;

public class BatchResult<T> {

	private EnumRange<T> m_failRange;

	private ContinuousRange m_doneRange;

	public BatchResult(EnumRange<T> failRange, ContinuousRange doneRange) {
		m_failRange = failRange;
		m_doneRange = doneRange;
	}

	public EnumRange<T> getFailRange() {
		return m_failRange;
	}

	public ContinuousRange getDoneRange() {
		return m_doneRange;
	}

	public void merge(BatchResult<T> resultToMerge) {
		EnumRange<T> failRangeToMerge = resultToMerge.getFailRange();
		if (m_failRange == null) {
			m_failRange = failRangeToMerge;
		} else {
			if (failRangeToMerge != null) {
				for (Pair<Long, T> newOffset : failRangeToMerge.getOffsets()) {
					m_failRange.addOffset(newOffset);
				}
			}
		}

		ContinuousRange doneRangeToMerge = resultToMerge.getDoneRange();
		long newDoneStart = Math.min(m_doneRange.getStart(), doneRangeToMerge.getStart());
		long newDoneEnd = Math.max(m_doneRange.getEnd(), doneRangeToMerge.getEnd());
		m_doneRange = new ContinuousRange(newDoneStart, newDoneEnd);
	}

}
