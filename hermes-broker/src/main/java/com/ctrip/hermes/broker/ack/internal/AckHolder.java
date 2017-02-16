package com.ctrip.hermes.broker.ack.internal;

import java.util.List;

import org.unidal.tuple.Pair;

public interface AckHolder<T> {
	public static enum AckHolderType {
		NORMAL, FORWARD_ONLY;
	}

	void delivered(List<Pair<Long, T>> range, long develiveredTime);

	void acked(long offset, boolean success);

	BatchResult<T> scan();

	long getMaxAckedOffset();

}
