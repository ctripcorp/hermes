package com.ctrip.hermes.broker.queue;

import java.util.List;
import java.util.Map;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.broker.queue.DefaultMessageQueueManager.Operation;
import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.bo.SendMessageResult;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch;
import com.ctrip.hermes.core.selector.OffsetGenerator;
import com.ctrip.hermes.core.transport.command.MessageBatchWithRawData;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface MessageQueue extends OffsetGenerator {

	String getTopic();

	int getPartition();

	ListenableFuture<Map<Integer, SendMessageResult>> appendMessageAsync(boolean isPriority,
	      MessageBatchWithRawData batch, long expireTime);

	MessageQueueCursor getCursor(String groupId, Lease lease, Offset offset);

	Offset findLatestConsumerOffset(String groupId);

	Offset findMessageOffsetByTime(long time);

	TppConsumerMessageBatch findMessagesByOffsets(boolean isPriority, List<Long> offsets);

	void stop();

	void checkHolders();

	boolean offerAckHolderOp(Operation operation);

	boolean offerAckMessagesTask(AckMessagesTask task);

	Pair<Boolean, Long> flush(int maxMsgCount);

}
