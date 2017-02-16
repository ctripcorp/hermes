package com.ctrip.hermes.broker.queue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.broker.ack.internal.AckHolder.AckHolderType;
import com.ctrip.hermes.broker.queue.DefaultMessageQueueManager.Operation.OperationType;
import com.ctrip.hermes.broker.selector.SendMessageSelectorManager;
import com.ctrip.hermes.core.bo.AckContext;
import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.bo.SendMessageResult;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch.MessageMeta;
import com.ctrip.hermes.core.selector.CallbackContext;
import com.ctrip.hermes.core.selector.FixedExpireTimeHolder;
import com.ctrip.hermes.core.selector.SelectorCallback;
import com.ctrip.hermes.core.selector.Slot;
import com.ctrip.hermes.core.selector.TriggerResult;
import com.ctrip.hermes.core.selector.TriggerResult.State;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.core.transport.command.MessageBatchWithRawData;
import com.ctrip.hermes.core.transport.command.v2.AckMessageCommandV2;
import com.ctrip.hermes.core.utils.CollectionUtil;
import com.ctrip.hermes.core.utils.CollectionUtil.Transformer;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.env.config.broker.BrokerConfigProvider;
import com.google.common.util.concurrent.ListenableFuture;

@Named(type = MessageQueueManager.class)
public class DefaultMessageQueueManager extends ContainerHolder implements MessageQueueManager, Initializable {

	private static final Logger log = LoggerFactory.getLogger(DefaultMessageQueueManager.class);

	private static final Long MIN_LONG = Long.MIN_VALUE;

	@Inject
	private MessageQueuePartitionFactory m_queueFactory;

	@Inject
	private BrokerConfigProvider m_config;

	@Inject
	private SystemClockService m_systemClockService;

	@Inject
	private SendMessageSelectorManager m_selectorManager;

	// one <topic, partition, lease> mapping to one MessageQueue
	private Map<Pair<String, Integer>, MessageQueue> m_messageQueues = new ConcurrentHashMap<>();

	private AtomicBoolean m_stopped = new AtomicBoolean(false);

	private ScheduledExecutorService m_ackOpExecutor;

	private ScheduledExecutorService m_ackMessagesTaskExecutor;

	@Override
	public ListenableFuture<Map<Integer, SendMessageResult>> appendMessageAsync(final String topic, final int partition,
	      boolean priority, final MessageBatchWithRawData data, long expireTime) {
		if (!m_stopped.get()) {
			MessageQueue mq = getOrInitializeMessageQueue(topic, partition);

			ListenableFuture<Map<Integer, SendMessageResult>> f;

			// generate selector offset and add to "wait queue" should be atomic
			synchronized (mq) {
				data.setSelectorOffset(mq.nextOffset(data.getMsgSeqs().isEmpty() ? 1 : data.getMsgSeqs().size()));
				f = mq.appendMessageAsync(priority, data, expireTime);
			}

			m_selectorManager.getSelector().update(new Pair<>(topic, partition), true,
			      new Slot(0, data.getSelectorOffset()));
			return f;
		} else {
			return null;
		}
	}

	@Override
	public MessageQueueCursor getCursor(Tpg tpg, Lease lease, Offset offset) {
		if (!m_stopped.get()) {
			return getOrInitializeMessageQueue(tpg.getTopic(), tpg.getPartition()).getCursor(tpg.getGroupId(), lease,
			      offset);
		} else {
			return null;
		}
	}

	private MessageQueue getOrInitializeMessageQueue(final String topic, final int partition) {
		Pair<String, Integer> key = new Pair<>(topic, partition);
		if (!m_messageQueues.containsKey(key)) {
			synchronized (m_messageQueues) {
				if (!m_messageQueues.containsKey(key)) {
					MessageQueue mq = m_queueFactory.getMessageQueue(topic, partition, m_ackOpExecutor,
					      m_ackMessagesTaskExecutor);
					m_messageQueues.put(key, mq);

					m_selectorManager.register(new Pair<>(topic, partition), new FixedExpireTimeHolder(Long.MAX_VALUE),
					      new SelectorCallback() {

						      @Override
						      public void onReady(CallbackContext ctx) {
							      flush(topic, partition, ctx);
						      }
					      }, null, mq.nextOffset(1));
				}
			}
		}

		return m_messageQueues.get(key);
	}

	@Override
	public void stop() {
		if (m_stopped.compareAndSet(false, true)) {
			m_ackOpExecutor.shutdown();
			m_ackMessagesTaskExecutor.shutdown();

			while (!m_ackOpExecutor.isTerminated()) {
				try {
					m_ackOpExecutor.awaitTermination(1, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					// ignore
				}
			}

			while (!m_ackMessagesTaskExecutor.isTerminated()) {
				try {
					m_ackMessagesTaskExecutor.awaitTermination(1, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					// ignore
				}
			}

			// m_flushCheckerTask.stop();
			// TODO [selector] flush remaining messages

			for (MessageQueue mq : m_messageQueues.values()) {
				mq.stop();
			}

		}
	}

	@Override
	public void delivered(TppConsumerMessageBatch batch, String groupId, boolean withOffset,
	      boolean needServerSideAckHolder) {
		// TODO remove legacy code
		if (needServerSideAckHolder) {
			if (m_stopped.get()) {
				return;
			}

			Tpp tpp = new Tpp(batch.getTopic(), batch.getPartition(), batch.isPriority());
			resetPriorityIfResend(tpp, batch.isResend());
			Pair<Boolean, String> key = new Pair<>(tpp.isPriority(), groupId);
			List<Pair<Long, MessageMeta>> msgId2Metas = new ArrayList<>(batch.getMessageMetas().size());
			for (MessageMeta msgMeta : batch.getMessageMetas()) {
				msgId2Metas.add(new Pair<>(msgMeta.getId(), msgMeta));
			}

			AckHolderType ackHolderType = withOffset ? AckHolderType.FORWARD_ONLY : AckHolderType.NORMAL;
			boolean offered = getOrInitializeMessageQueue(tpp.getTopic(), tpp.getPartition()).offerAckHolderOp(
			      new Operation(key, batch.isResend(), OperationType.DELIVERED, ackHolderType, msgId2Metas,
			            m_systemClockService.now()));

			logIfOfferFail("delivered", offered);
		}
	}

	@Override
	public void acked(Tpp tpp, String groupId, boolean resend, List<AckContext> ackContexts, int ackType) {
		if (m_stopped.get()) {
			return;
		}
		resetPriorityIfResend(tpp, resend);
		Pair<Boolean, String> key = new Pair<>(tpp.isPriority(), groupId);
		for (AckContext context : ackContexts) {
			boolean offered = getOrInitializeMessageQueue(tpp.getTopic(), tpp.getPartition()).offerAckHolderOp(
			      new Operation(key, resend, OperationType.ACK, getHolderType(ackType), context.getMsgSeq(),
			            m_systemClockService.now()));
			logIfOfferFail("acked", offered);
		}
	}

	@Override
	public void nacked(Tpp tpp, String groupId, boolean resend, List<AckContext> nackContexts, int ackType) {
		if (m_stopped.get()) {
			return;
		}
		resetPriorityIfResend(tpp, resend);
		Pair<Boolean, String> key = new Pair<>(tpp.isPriority(), groupId);
		for (AckContext context : nackContexts) {
			boolean offered = getOrInitializeMessageQueue(tpp.getTopic(), tpp.getPartition()).offerAckHolderOp(
			      new Operation(key, resend, OperationType.NACK, getHolderType(ackType), context.getMsgSeq(),
			            m_systemClockService.now()));
			logIfOfferFail("nacked", offered);
		}
	}

	@Override
	public void submitAckMessagesTask(AckMessagesTask task) {
		if (m_stopped.get()) {
			return;
		}

		boolean offered = getOrInitializeMessageQueue(task.getTopic(), task.getPartition()).offerAckMessagesTask(task);
		logIfOfferFail("offerAckMessageTask", offered);

	}

	private AckHolderType getHolderType(int ackType) {
		return AckMessageCommandV2.FORWARD_ONLY == ackType ? AckHolderType.FORWARD_ONLY : AckHolderType.NORMAL;
	}

	private void logIfOfferFail(String type, boolean offered) {
		if (!offered) {
			log.warn("Operation queue full when doing {}", type);
		}
	}

	private void resetPriorityIfResend(Tpp tpp, boolean resend) {
		if (resend) {
			tpp.setPriority(false);
		}
	}

	public static class Operation {
		public enum OperationType {
			ACK, NACK, DELIVERED;
		}

		// priority, group
		private Pair<Boolean, String> m_key;

		private boolean m_resend;

		private Object m_data;

		private OperationType m_operationType;

		private long m_createTime;

		private AckHolderType m_ackHolderType = AckHolderType.NORMAL;

		Operation(Pair<Boolean, String> key, boolean isResend, OperationType operationType, AckHolderType ackHolderType,
		      Object data, long createTime) {
			m_key = key;
			m_resend = isResend;
			m_data = data;
			m_operationType = operationType;
			m_ackHolderType = ackHolderType;
			m_createTime = createTime;
		}

		public boolean isResend() {
			return m_resend;
		}

		public Pair<Boolean, String> getKey() {
			return m_key;
		}

		public Object getData() {
			return m_data;
		}

		public OperationType getType() {
			return m_operationType;
		}

		public long getCreateTime() {
			return m_createTime;
		}

		public AckHolderType getAckHolderType() {
			return m_ackHolderType;
		}

	}

	@Override
	public void initialize() throws InitializationException {
		m_ackOpExecutor = Executors.newScheduledThreadPool(m_config.getAckOpExecutorThreadCount(),
		      HermesThreadFactory.create("AckOp", true));
		m_ackMessagesTaskExecutor = Executors.newScheduledThreadPool(m_config.getAckMessagesTaskExecutorThreadCount(),
		      HermesThreadFactory.create("AckMessagesTaskExecutor", true));
	}

	@Override
	public Offset findLatestConsumerOffset(Tpg tpg) {
		if (!m_stopped.get()) {
			return getOrInitializeMessageQueue(tpg.getTopic(), tpg.getPartition()).findLatestConsumerOffset(
			      tpg.getGroupId());
		}
		return null;
	}

	@Override
	public Offset findMessageOffsetByTime(String topic, int partition, long time) {
		if (!m_stopped.get()) {
			return getOrInitializeMessageQueue(topic, partition).findMessageOffsetByTime(time);
		}
		return null;
	}

	@Override
	@SuppressWarnings("unchecked")
	public List<TppConsumerMessageBatch> findMessagesByOffsets(String topic, int partition, List<Offset> offsets) {
		List<TppConsumerMessageBatch> result = new ArrayList<TppConsumerMessageBatch>();
		MessageQueue queue = getOrInitializeMessageQueue(topic, partition);
		if (!m_stopped.get()) {
			if (queue != null) {
				TppConsumerMessageBatch priorityMessasges = queue.findMessagesByOffsets(true,
				      (List<Long>) CollectionUtil.collect(offsets, new Transformer() {
					      @Override
					      public Object transform(Object offset) {
						      return ((Offset) offset).getPriorityOffset();
					      }
				      }));
				if (priorityMessasges != null) {
					result.add(priorityMessasges);
				}
				TppConsumerMessageBatch nonPriorityMessages = queue.findMessagesByOffsets(false,
				      (List<Long>) CollectionUtil.collect(offsets, new Transformer() {
					      @Override
					      public Object transform(Object offset) {
						      return ((Offset) offset).getNonPriorityOffset();
					      }
				      }));
				if (nonPriorityMessages != null) {
					result.add(nonPriorityMessages);
				}
			}
		}
		return result;
	}

	private void flush(final String topic, final int partition, CallbackContext ctx) {
		Pair<Boolean, Long> flushResult = new Pair<>(false, MIN_LONG);
		try {
			final Pair<String, Integer> tp = new Pair<>(topic, partition);
			MessageQueue mq = m_messageQueues.get(tp);

			if (m_config.isMessageQueueFlushLimitDynamicAdjust(topic)) {
				//TODO temporary use fix value
				flushResult = mq.flush(m_config.getMessageQueueFlushCountLimit(topic));
			} else {
				flushResult = mq.flush(m_config.getMessageQueueFlushCountLimit(topic));
			}
		} catch (Exception e) {
			log.error("Unexpected exception ", e);
		} finally {
			boolean hasUnflushedMessagesBeforeFlush = flushResult.getKey();
			long maxPurgedOrSavedSelectorOffset = flushResult.getValue();

			State state;
			if (hasUnflushedMessagesBeforeFlush) {
				if (maxPurgedOrSavedSelectorOffset > 0) {
					state = State.GotAndSuccessfullyProcessed;
				} else {
					// flush fail, stop flush until trigger by safe trigger
					state = State.GotButErrorInProcessing;
				}
			} else {
				state = State.GotNothing;
			}

			TriggerResult triggerResult = new TriggerResult(state, new long[] { maxPurgedOrSavedSelectorOffset });

			m_selectorManager.reRegister(new Pair<>(topic, partition), ctx, triggerResult, new FixedExpireTimeHolder(
			      Long.MAX_VALUE), new SelectorCallback() {

				@Override
				public void onReady(CallbackContext innerCtx) {
					flush(topic, partition, innerCtx);
				}
			});
		}
	}

}
