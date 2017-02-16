package com.ctrip.hermes.broker.queue;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.broker.queue.storage.FetchResult;
import com.ctrip.hermes.broker.selector.PullMessageSelectorManager;
import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.selector.CallbackContext;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public abstract class AbstractMessageQueueCursor implements MessageQueueCursor {
	protected static final Logger log = LoggerFactory.getLogger(AbstractMessageQueueCursor.class);

	protected static final int STATE_NOT_INITED = 0;

	protected static final int STATE_INITING = 1;

	protected static final int STATE_INITED = 2;

	protected static final int STATE_INIT_ERROR = 0;

	protected static LoadingCache<Tpg, Long> m_lastPriorityTriggerTime;

	protected static LoadingCache<Tpg, Long> m_lastNonPriorityTriggerTime;

	protected static LoadingCache<Tpg, Long> m_lastResendTriggerTime;

	protected Tpg m_tpg;

	protected Tpp m_priorityTpp;

	protected Tpp m_nonPriorityTpp;

	protected Object m_priorityOffset;

	protected Object m_nonPriorityOffset;

	protected Object m_resendOffset;

	protected MetaService m_metaService;

	protected int m_groupIdInt;

	protected Lease m_lease;

	protected AtomicInteger m_state = new AtomicInteger(STATE_NOT_INITED);

	protected AtomicBoolean m_stopped = new AtomicBoolean(false);

	protected MessageQueue m_messageQueue;

	protected long m_priorityMessageFetchBySafeTriggerMinInterval;

	protected long m_nonpriorityMessageFetchBySafeTriggerMinInterval;

	protected long m_resendMessageFetchBySafeTriggerMinInterval;

	static {
		m_lastPriorityTriggerTime = CacheBuilder.newBuilder().maximumSize(5000).build(new CacheLoader<Tpg, Long>() {

			@Override
			public Long load(Tpg key) throws Exception {
				return -1L;
			}
		});
		m_lastNonPriorityTriggerTime = CacheBuilder.newBuilder().maximumSize(5000).build(new CacheLoader<Tpg, Long>() {

			@Override
			public Long load(Tpg key) throws Exception {
				return -1L;
			}
		});
		m_lastResendTriggerTime = CacheBuilder.newBuilder().maximumSize(5000).build(new CacheLoader<Tpg, Long>() {

			@Override
			public Long load(Tpg key) throws Exception {
				return -1L;
			}
		});
	}

	public AbstractMessageQueueCursor(Tpg tpg, Lease lease, MetaService metaService, MessageQueue messageQueue,
	      long priorityMsgFetchBySafeTriggerMinInterval, long nonpriorityMsgFetchBySafeTriggerMinInterval,
	      long resendMsgFetchBySafeTriggerMinInterval) {
		m_tpg = tpg;
		m_lease = lease;
		m_priorityTpp = new Tpp(tpg.getTopic(), tpg.getPartition(), true);
		m_nonPriorityTpp = new Tpp(tpg.getTopic(), tpg.getPartition(), false);
		m_metaService = metaService;
		m_groupIdInt = m_metaService.translateToIntGroupId(m_tpg.getTopic(), m_tpg.getGroupId());
		m_messageQueue = messageQueue;
		m_priorityMessageFetchBySafeTriggerMinInterval = priorityMsgFetchBySafeTriggerMinInterval;
		m_nonpriorityMessageFetchBySafeTriggerMinInterval = nonpriorityMsgFetchBySafeTriggerMinInterval;
		m_resendMessageFetchBySafeTriggerMinInterval = resendMsgFetchBySafeTriggerMinInterval;
	}

	@Override
	public void init() {
		if (m_state.compareAndSet(STATE_NOT_INITED, STATE_INITING)) {
			try {
				m_priorityOffset = loadLastPriorityOffset();
				m_nonPriorityOffset = loadLastNonPriorityOffset();
				m_resendOffset = loadLastResendOffset();
				m_state.set(STATE_INITED);
			} catch (Exception e) {
				log.error("Failed to init cursor", e);
				m_state.set(STATE_INIT_ERROR);
				throw e;
			}
		}
	}

	public Lease getLease() {
		return m_lease;
	}

	public boolean hasError() {
		return m_state.get() == STATE_INIT_ERROR;
	}

	public boolean isInited() {
		return m_state.get() == STATE_INITED;
	}

	protected abstract void doStop();

	protected abstract Object loadLastPriorityOffset();

	protected abstract Object loadLastNonPriorityOffset();

	protected abstract Object loadLastResendOffset();

	protected abstract FetchResult fetchPriorityMessages(int batchSize, String filter);

	protected abstract FetchResult fetchNonPriorityMessages(int batchSize, String filter);

	protected abstract FetchResult fetchResendMessages(int batchSize);

	@Override
	@SuppressWarnings("unchecked")
	public synchronized Pair<Offset, List<TppConsumerMessageBatch>> next(int batchSize, String filter,
	      CallbackContext callbackCtx) {
		List<TppConsumerMessageBatch> batches = doNext(batchSize, filter, callbackCtx);
		Offset offset = new Offset((long) m_priorityOffset, (long) m_nonPriorityOffset, (Pair<Date, Long>) m_resendOffset);

		return new Pair<Offset, List<TppConsumerMessageBatch>>(batches == null ? null : offset, batches);
	}

	protected List<TppConsumerMessageBatch> doNext(int batchSize, String filter, CallbackContext callbackCtx) {
		if (m_stopped.get() || m_lease.isExpired()) {
			return null;
		}

		try {
			List<TppConsumerMessageBatch> result = new LinkedList<>();
			int remainingSize = batchSize;

			long now = System.currentTimeMillis();

			if (shouldFetchPriorityMessages(callbackCtx, now)) {
				FetchResult pFetchResult = fetchPriorityMessages(batchSize, filter);
				m_lastPriorityTriggerTime.put(m_tpg, now);

				if (pFetchResult != null) {
					TppConsumerMessageBatch priorityMessageBatch = pFetchResult.getBatch();
					if (priorityMessageBatch != null && priorityMessageBatch.size() > 0) {
						result.add(priorityMessageBatch);
						remainingSize -= priorityMessageBatch.size();
						m_priorityOffset = pFetchResult.getOffset();
					}
				}
			}

			if (shouldFetchResendMessages(callbackCtx, now) && remainingSize > 0) {
				FetchResult rFetchResult = fetchResendMessages(remainingSize);
				m_lastResendTriggerTime.put(m_tpg, now);

				if (rFetchResult != null) {
					TppConsumerMessageBatch resendMessageBatch = rFetchResult.getBatch();
					if (resendMessageBatch != null && resendMessageBatch.size() > 0) {
						result.add(resendMessageBatch);
						remainingSize -= resendMessageBatch.size();
						m_resendOffset = rFetchResult.getOffset();
					}
				}
			}

			if (shouldFetchNonPriorityMessages(callbackCtx, now) && remainingSize > 0) {
				FetchResult npFetchResult = fetchNonPriorityMessages(remainingSize, filter);
				m_lastNonPriorityTriggerTime.put(m_tpg, now);

				if (npFetchResult != null) {
					TppConsumerMessageBatch nonPriorityMessageBatch = npFetchResult.getBatch();
					if (nonPriorityMessageBatch != null && nonPriorityMessageBatch.size() > 0) {
						result.add(nonPriorityMessageBatch);
						remainingSize -= nonPriorityMessageBatch.size();
						m_nonPriorityOffset = npFetchResult.getOffset();
					}
				}
			}

			return result;
		} catch (Exception e) {
			// TODO
		}

		return null;
	}

	protected boolean shouldFetchPriorityMessages(CallbackContext callbackCtx, long now) {
		return callbackCtx.getSlotMatchResults()[PullMessageSelectorManager.SLOT_PRIORITY_INDEX].isMatch()
		      || (callbackCtx.getSlotMatchResults()[PullMessageSelectorManager.SLOT_RESEND_INDEX].isMatch() && now
		            - m_lastPriorityTriggerTime.getUnchecked(m_tpg) > m_priorityMessageFetchBySafeTriggerMinInterval);
	}

	protected boolean shouldFetchNonPriorityMessages(CallbackContext callbackCtx, long now) {
		return callbackCtx.getSlotMatchResults()[PullMessageSelectorManager.SLOT_NONPRIORITY_INDEX].isMatch()
		      || (callbackCtx.getSlotMatchResults()[PullMessageSelectorManager.SLOT_RESEND_INDEX].isMatch() && now
		            - m_lastNonPriorityTriggerTime.getUnchecked(m_tpg) > m_nonpriorityMessageFetchBySafeTriggerMinInterval);
	}

	protected boolean shouldFetchResendMessages(CallbackContext callbackCtx, long now) {
		return now - m_lastResendTriggerTime.getUnchecked(m_tpg) > m_resendMessageFetchBySafeTriggerMinInterval
		      && callbackCtx.getSlotMatchResults()[PullMessageSelectorManager.SLOT_RESEND_INDEX].isMatch();
	}

	@Override
	public void stop() {
		if (m_stopped.compareAndSet(false, true)) {
			doStop();
		}
	}

}
