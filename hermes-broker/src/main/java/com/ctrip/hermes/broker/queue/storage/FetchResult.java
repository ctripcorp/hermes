package com.ctrip.hermes.broker.queue.storage;

import com.ctrip.hermes.core.message.TppConsumerMessageBatch;

public class FetchResult {
	private TppConsumerMessageBatch batch;

	private Object offset;

	public TppConsumerMessageBatch getBatch() {
		return batch;
	}

	public void setBatch(TppConsumerMessageBatch batch) {
		this.batch = batch;
	}

	public Object getOffset() {
		return offset;
	}

	public void setOffset(Object offset) {
		this.offset = offset;
	}

}
