package com.ctrip.hermes.broker.queue;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.broker.queue.storage.FetchResult;
import com.ctrip.hermes.broker.queue.storage.MessageQueueStorage;
import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch.MessageMeta;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.selector.DefaultOffsetGenerator;
import com.ctrip.hermes.core.selector.OffsetGenerator;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class DefaultMessageQueue extends AbstractMessageQueue {
	private static final Logger log = LoggerFactory.getLogger(DefaultMessageQueue.class);

	private MetaService m_metaService;

	private OffsetGenerator m_offsetGenerator = new DefaultOffsetGenerator();

	public DefaultMessageQueue(String topic, int partition, MessageQueueStorage storage, MetaService metaService,
	      ScheduledExecutorService ackOpExecutor, ScheduledExecutorService ackMessagesTaskExecutor) {
		super(topic, partition, storage, ackOpExecutor, ackMessagesTaskExecutor);
		m_metaService = metaService;
	}

	@Override
	protected MessageQueueCursor create(String groupId, Lease lease, Offset offset) {
		if (offset == null) {
			return new DefaultMessageQueueCursor(new Tpg(m_topic, m_partition, groupId), lease, m_storage, m_metaService,
			      this, m_config.getMessageQueueFetchPriorityMessageBySafeTriggerMinInterval(),
			      m_config.getMessageQueueFetchNonPriorityMessageBySafeTriggerMinInterval(),
			      m_config.getMessageQueueFetchResendMessageBySafeTriggerMinInterval());
		} else {
			return new DefaultMessageQueueCursorV2(new Tpg(m_topic, m_partition, groupId), lease, m_storage,
			      m_metaService, this, offset, m_config.getMessageQueueFetchPriorityMessageBySafeTriggerMinInterval(),
			      m_config.getMessageQueueFetchNonPriorityMessageBySafeTriggerMinInterval(),
			      m_config.getMessageQueueFetchResendMessageBySafeTriggerMinInterval());
		}
	}

	@Override
	protected void doNack(boolean resend, boolean isPriority, String groupId, List<Pair<Long, MessageMeta>> msgId2Metas) {
		m_storage.nack(new Tpp(m_topic, m_partition, isPriority), groupId, resend, msgId2Metas);
	}

	@Override
	protected void doAck(boolean resend, boolean isPriority, String groupId, long msgSeq) {
		m_storage.ack(new Tpp(m_topic, m_partition, isPriority), groupId, resend, msgSeq);
	}

	@Override
	protected void doStop() {
	}

	@Override
	public Offset findLatestConsumerOffset(String groupId) {
		int groupIdInt = m_metaService.translateToIntGroupId(m_topic, groupId);
		try {
			long pOffset = (long) m_storage.findLastOffset(new Tpp(m_topic, m_partition, true), groupIdInt);

			long npOffset = (long) m_storage.findLastOffset(new Tpp(m_topic, m_partition, false), groupIdInt);

			@SuppressWarnings("unchecked")
			Pair<Date, Long> rOffset = (Pair<Date, Long>) m_storage.findLastResendOffset(new Tpg(m_topic, m_partition,
			      groupId));
			return new Offset(pOffset, npOffset, rOffset);
		} catch (Exception e) {
			log.error("Find latest offset failed: topic= {}, partition= {}, group= {}.", m_topic, m_partition, groupId, e);
		}
		return null;
	}

	@Override
	public Offset findMessageOffsetByTime(long time) {
		long pOffset = (long) m_storage.findMessageOffsetByTime(new Tpp(m_topic, m_partition, true), time);
		long npOffset = (long) m_storage.findMessageOffsetByTime(new Tpp(m_topic, m_partition, false), time);
		return new Offset(pOffset, npOffset, null);
	}

	@Override
	public TppConsumerMessageBatch findMessagesByOffsets(boolean isPriority, List<Long> offsets) {
		Tpp tpp = new Tpp(m_topic, m_partition, isPriority);
		FetchResult fetchResult = m_storage.fetchMessages(tpp, new ArrayList<Object>(offsets));
		return fetchResult == null ? null : fetchResult.getBatch();
	}

	@Override
	public long nextOffset(int delta) {
		return m_offsetGenerator.nextOffset(delta);
	}
}
