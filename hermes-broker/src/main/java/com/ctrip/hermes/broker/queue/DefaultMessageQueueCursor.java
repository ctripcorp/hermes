package com.ctrip.hermes.broker.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.broker.queue.storage.FetchResult;
import com.ctrip.hermes.broker.queue.storage.MessageQueueStorage;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.meta.MetaService;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class DefaultMessageQueueCursor extends AbstractMessageQueueCursor {

	private static final Logger log = LoggerFactory.getLogger(DefaultMessageQueueCursor.class);

	private MessageQueueStorage m_storage;

	public DefaultMessageQueueCursor(Tpg tpg, Lease lease, MessageQueueStorage storage, MetaService metaService,
	      MessageQueue messageQueue, long priorityMsgFetchBySafeTriggerMinInterval,
	      long nonpriorityMsgFetchBySafeTriggerMinInterval, long resendMsgFetchBySafeTriggerMinInterval) {
		super(tpg, lease, metaService, messageQueue, priorityMsgFetchBySafeTriggerMinInterval,
		      nonpriorityMsgFetchBySafeTriggerMinInterval, resendMsgFetchBySafeTriggerMinInterval);
		m_storage = storage;
	}

	@Override
	protected Object loadLastPriorityOffset() {
		try {
			return m_storage.findLastOffset(m_priorityTpp, m_groupIdInt);
		} catch (Exception e) {
			throw new RuntimeException(String.format(
			      "Load priority message queue offset failed.(topic=%s, partition=%d, groupId=%d)", m_tpg.getTopic(),
			      m_tpg.getPartition(), m_groupIdInt), e);
		}
	}

	@Override
	protected Object loadLastNonPriorityOffset() {
		try {
			return m_storage.findLastOffset(m_nonPriorityTpp, m_groupIdInt);
		} catch (Exception e) {
			throw new RuntimeException(String.format(
			      "Load non-priority message queue offset failed.(topic=%s, partition=%d, groupId=%d)", m_tpg.getTopic(),
			      m_tpg.getPartition(), m_groupIdInt), e);
		}
	}

	@Override
	protected Object loadLastResendOffset() {
		try {
			return m_storage.findLastResendOffset(m_tpg);
		} catch (Exception e) {
			throw new RuntimeException(String.format(
			      "Load resend message queue offset failed.(topic=%s, partition=%d, groupId=%d)", m_tpg.getTopic(),
			      m_tpg.getPartition(), m_groupIdInt), e);
		}
	}

	@Override
	protected FetchResult fetchPriorityMessages(int batchSize, String filter) {
		if (!m_stopped.get()) {
			try {
				return m_storage.fetchMessages(m_priorityTpp, m_priorityOffset, batchSize, filter);
			} catch (Exception e) {
				if (log.isDebugEnabled()) {
					log.debug("Fetch priority message failed. [{}]", m_priorityTpp, e);
				}
			}
		}
		return null;
	}

	@Override
	protected FetchResult fetchNonPriorityMessages(int batchSize, String filter) {
		if (!m_stopped.get()) {
			try {
				return m_storage.fetchMessages(m_nonPriorityTpp, m_nonPriorityOffset, batchSize, filter);
			} catch (Exception e) {
				if (log.isDebugEnabled()) {
					log.debug("Fetch non priority message failed. [{}]", m_nonPriorityTpp, e);
				}
			}
		}
		return null;
	}

	@Override
	protected FetchResult fetchResendMessages(int batchSize) {
		if (!m_stopped.get()) {
			try {
				return m_storage.fetchResendMessages(m_tpg, m_resendOffset, batchSize);
			} catch (Exception e) {
				if (log.isDebugEnabled()) {
					log.debug("Fetch resend message failed. [{}]", m_tpg, e);
				}
			}
		}
		return null;
	}

	@Override
	protected void doStop() {

	}

}
