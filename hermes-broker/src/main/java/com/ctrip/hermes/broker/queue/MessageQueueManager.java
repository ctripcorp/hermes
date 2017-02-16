package com.ctrip.hermes.broker.queue;

import java.util.List;
import java.util.Map;

import com.ctrip.hermes.core.bo.AckContext;
import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.bo.SendMessageResult;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch;
import com.ctrip.hermes.core.transport.command.MessageBatchWithRawData;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * 
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface MessageQueueManager {

	public ListenableFuture<Map<Integer, SendMessageResult>> appendMessageAsync(String topic, int partition,
	      boolean priority, MessageBatchWithRawData data, long expireTime);

	public MessageQueueCursor getCursor(Tpg tpg, Lease lease, Offset offset);

	public List<TppConsumerMessageBatch> findMessagesByOffsets(String topic, int partition, List<Offset> offsets);

	public void stop();

	void delivered(TppConsumerMessageBatch batch, String groupId, boolean withOffset, boolean needServerSideAckHolder);

	// TODO remove legacy code
	void acked(Tpp tpp, String groupId, boolean resend, List<AckContext> ackContexts, int ackType);

	// TODO remove legacy code
	void nacked(Tpp tpp, String groupId, boolean resend, List<AckContext> nackContexts, int ackType);

	void submitAckMessagesTask(AckMessagesTask task);

	public Offset findLatestConsumerOffset(Tpg tpg);

	public Offset findMessageOffsetByTime(String topic, int partition, long time);

}
