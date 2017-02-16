package com.ctrip.hermes.consumer.pull;

import java.util.Collections;
import java.util.List;

import com.ctrip.hermes.consumer.api.OffsetCommitCallback;
import com.ctrip.hermes.consumer.api.PulledBatch;

@SuppressWarnings("rawtypes")
public enum DummyPulledBatch implements PulledBatch {

	INSTANCE;

	@Override
	public List getMessages() {
		return Collections.EMPTY_LIST;
	}

	@Override
	public void commitAsync() {
	}

	@SuppressWarnings("unchecked")
	@Override
	public void commitAsync(OffsetCommitCallback callback) {
		callback.onComplete(Collections.EMPTY_MAP, null);
	}

	@Override
	public void commitSync() {
	}

	@Override
	public long getBornTime() {
		return -1L;
	}

}
