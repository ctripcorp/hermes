package com.ctrip.hermes.consumer.integration.assist;

import java.util.List;

/***
 * Test messages' creator, create a list of batch of messages to simulate PullMessageCommandResult
 */
public interface RawMessageCreator<T> {
	/***
	 * @return List of batch of message
	 */
	public List<List<T>> createRawMessages();
}
