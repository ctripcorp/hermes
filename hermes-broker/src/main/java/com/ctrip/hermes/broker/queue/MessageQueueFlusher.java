package com.ctrip.hermes.broker.queue;

import java.util.Map;

import com.ctrip.hermes.core.bo.SendMessageResult;
import com.ctrip.hermes.core.transport.command.MessageBatchWithRawData;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface MessageQueueFlusher {
	boolean hasUnflushedMessages();

	long flush(int msgCount);

	ListenableFuture<Map<Integer, SendMessageResult>> append(boolean isPriority, MessageBatchWithRawData batch,
	      long expireTime);
}
