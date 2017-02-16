package com.ctrip.hermes.broker.queue.storage;

import java.util.Collection;
import java.util.List;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch.MessageMeta;
import com.ctrip.hermes.core.transport.command.MessageBatchWithRawData;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface MessageQueueStorage {

	void appendMessages(String topic, int partition, boolean priority, Collection<MessageBatchWithRawData> batches)
	      throws Exception;

	Object findLastOffset(Tpp tpp, int groupId) throws Exception;

	Object findLastResendOffset(Tpg tpg) throws Exception;

	Object findMessageOffsetByTime(Tpp tpp, long time);

	FetchResult fetchMessages(Tpp tpp, Object startOffset, int batchSize, String filter);

	FetchResult fetchMessages(Tpp tpp, List<Object> offsets);

	FetchResult fetchResendMessages(Tpg tpg, Object startOffset, int batchSize);

	void nack(Tpp tpp, String groupId, boolean resend, List<Pair<Long, MessageMeta>> msgId2Metas);

	void ack(Tpp tpp, String groupId, boolean resend, long msgSeq);
}
