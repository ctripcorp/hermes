package com.ctrip.hermes.consumer.api;

import java.util.List;

import com.ctrip.hermes.core.message.ConsumerMessage;

public interface PulledBatch<T> {

	List<ConsumerMessage<T>> getMessages();

	/**
	 * Commit these messages asynchronously
	 */
	void commitAsync();

	void commitAsync(OffsetCommitCallback callback);

	/**
	 * Commit these messages synchronously
	 */
	void commitSync();

	long getBornTime();

}
