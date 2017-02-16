package com.ctrip.hermes.consumer.api;


public interface PullConsumerHolder<T> extends AutoCloseable {

	/**
	 * Attempt to fetch messages, return as soon as some messages are got.
	 * 
	 * @param maxMessageCount
	 *           max number of messages to return
	 * @param timeout
	 *           time in milliseconds to wait if no messages are available
	 * @return
	 */
	PulledBatch<T> poll(int maxMessageCount, int timeout);

	/**
	 * Attempt to fetch messages, wait until {@code expectedMessageCount} are got or timeout
	 * 
	 * @param expectedMessageCount
	 *           messages expected to fetch
	 * @param timeout
	 *           time in milliseconds to wait if message count is below {@code expectedMessageCount}
	 * @return
	 */
	PulledBatch<T> collect(int expectedMessageCount, int timeout);

}
