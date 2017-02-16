package com.ctrip.hermes.broker.queue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.broker.ack.internal.AckHolder;
import com.ctrip.hermes.broker.ack.internal.AckHolder.AckHolderType;
import com.ctrip.hermes.broker.ack.internal.BatchResult;
import com.ctrip.hermes.broker.ack.internal.ContinuousRange;
import com.ctrip.hermes.broker.ack.internal.DefaultAckHolder;
import com.ctrip.hermes.broker.ack.internal.EnumRange;
import com.ctrip.hermes.broker.ack.internal.ForwardOnlyAckHolder;
import com.ctrip.hermes.broker.queue.DefaultMessageQueueManager.Operation;
import com.ctrip.hermes.broker.queue.storage.MessageQueueStorage;
import com.ctrip.hermes.core.bo.AckContext;
import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.bo.SendMessageResult;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch.MessageMeta;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.selector.CallbackContext;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.core.transport.ChannelUtils;
import com.ctrip.hermes.core.transport.command.Command;
import com.ctrip.hermes.core.transport.command.MessageBatchWithRawData;
import com.ctrip.hermes.core.transport.command.v3.AckMessageResultCommandV3;
import com.ctrip.hermes.core.transport.command.v5.AckMessageResultCommandV5;
import com.ctrip.hermes.core.utils.CollectionUtil;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.env.config.broker.BrokerConfigProvider;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public abstract class AbstractMessageQueue implements MessageQueue {

	private static final Logger log = LoggerFactory.getLogger(AbstractMessageQueue.class);

	protected String m_topic;

	protected int m_partition;

	protected MessageQueueStorage m_storage;

	protected ConcurrentMap<String, AtomicReference<MessageQueueCursor>> m_cursors = new ConcurrentHashMap<>();

	protected AtomicBoolean m_stopped = new AtomicBoolean(false);

	// TODO while consumer disconnect, clear holder and offset
	protected Map<Pair<Boolean, String>, AckHolder<MessageMeta>> m_ackHolders;

	protected Map<Pair<Boolean, String>, AckHolder<MessageMeta>> m_resendAckHolders;

	protected Map<Pair<Boolean, String>, AckHolder<MessageMeta>> m_forwardOnlyAckHolders;

	private BlockingQueue<Operation> m_opQueue;

	private AckOpTaskHandler m_ackOpTaskHandler;

	private BlockingQueue<AckMessagesTask> m_ackMessageTaskQueue;

	private AckMessagesTaskHandler m_ackMsgsTaskHandler;

	protected BrokerConfigProvider m_config;

	private MetaService m_metaService;

	private ScheduledExecutorService m_ackOpExecutor;

	private ScheduledExecutorService m_ackMessagesTaskExecutor;

	private MessageQueueFlusher m_flusher;

	private SystemClockService m_systemClockService;

	public AbstractMessageQueue(String topic, int partition, MessageQueueStorage storage,
	      ScheduledExecutorService ackOpExecutor, ScheduledExecutorService ackMessagesTaskExecutor) {
		m_topic = topic;
		m_partition = partition;
		m_storage = storage;
		m_ackHolders = new ConcurrentHashMap<>();
		m_resendAckHolders = new ConcurrentHashMap<>();
		m_forwardOnlyAckHolders = new ConcurrentHashMap<>();
		m_ackOpExecutor = ackOpExecutor;
		m_ackMessagesTaskExecutor = ackMessagesTaskExecutor;
		m_config = PlexusComponentLocator.lookup(BrokerConfigProvider.class);
		m_metaService = PlexusComponentLocator.lookup(MetaService.class);
		m_flusher = new DefaultMessageQueueFlusher(m_topic, m_partition, m_storage, m_metaService);
		m_systemClockService = PlexusComponentLocator.lookup(SystemClockService.class);

		init();
	}

	private void init() {
		m_opQueue = new LinkedBlockingQueue<>(m_config.getAckOpQueueSize());
		m_ackOpTaskHandler = new AckOpTaskHandler();
		m_ackOpExecutor.schedule(m_ackOpTaskHandler, m_config.getAckOpCheckIntervalMillis(), TimeUnit.MILLISECONDS);

		m_ackMessageTaskQueue = new LinkedBlockingQueue<>(m_config.getAckMessagesTaskQueueSize());
		m_ackMsgsTaskHandler = new AckMessagesTaskHandler();
		m_ackMessagesTaskExecutor.schedule(m_ackMsgsTaskHandler,
		      m_config.getAckMessagesTaskExecutorCheckIntervalMillis(), TimeUnit.MILLISECONDS);
	}

	public String getTopic() {
		return m_topic;
	}

	public int getPartition() {
		return m_partition;
	}

	@Override
	public ListenableFuture<Map<Integer, SendMessageResult>> appendMessageAsync(boolean isPriority,
	      MessageBatchWithRawData batch, long expireTime) {
		if (m_stopped.get()) {
			return null;
		}

		return m_flusher.append(isPriority, batch, expireTime);
	}

	@Override
	public Pair<Boolean, Long> flush(final int maxMsgCount) {
		long maxPurgedOrSavedSelectorOffset = Long.MIN_VALUE;
		boolean hasUnflushedMessagesBeforeFlush = m_flusher.hasUnflushedMessages();

		if (hasUnflushedMessagesBeforeFlush) {
			try {
				maxPurgedOrSavedSelectorOffset = m_flusher.flush(maxMsgCount);
			} catch (RuntimeException e) {
				log.error("Exception occurred while flushing message queue(topic={}, partition={})", m_topic, m_partition,
				      e);
			}
		}

		return new Pair<>(hasUnflushedMessagesBeforeFlush, maxPurgedOrSavedSelectorOffset);
	}

	@Override
	public MessageQueueCursor getCursor(String groupId, Lease lease, Offset offset) {
		if (m_stopped.get()) {
			return null;
		}

		if (offset == null) {
			// TODO remove legacy code, getCursor must pass offset in the latest
			// version
			m_cursors.putIfAbsent(groupId, new AtomicReference<MessageQueueCursor>(null));

			MessageQueueCursor existingCursor = m_cursors.get(groupId).get();

			if (existingCursor == null || existingCursor.getLease().getId() != lease.getId() || existingCursor.hasError()) {
				MessageQueueCursor newCursor = create(groupId, lease, null);
				if (m_cursors.get(groupId).compareAndSet(existingCursor, newCursor)) {
					clearHolders(groupId);
					newCursor.init();
				}
			}

			MessageQueueCursor cursor = m_cursors.get(groupId).get();

			return cursor.isInited() ? new PooledMessageQueueCursor(cursor) : new NoopMessageQueueCursor();

		} else {
			MessageQueueCursor cursor = create(groupId, lease, offset);
			cursor.init();
			return cursor;
		}

	}

	// TODO remove legacy code
	private static class PooledMessageQueueCursor implements MessageQueueCursor {
		private MessageQueueCursor m_cursor;

		public PooledMessageQueueCursor(MessageQueueCursor cursor) {
			m_cursor = cursor;
		}

		@Override
		public Pair<Offset, List<TppConsumerMessageBatch>> next(int batchSize, String filter, CallbackContext callbackCtx) {
			return m_cursor.next(batchSize, filter, callbackCtx);
		}

		@Override
		public void init() {
			// do nothing
		}

		@Override
		public Lease getLease() {
			return m_cursor.getLease();
		}

		@Override
		public boolean hasError() {
			return m_cursor.hasError();
		}

		@Override
		public boolean isInited() {
			return m_cursor.isInited();
		}

		@Override
		public void stop() {
			// do nothing
		}
	}

	private void clearHolders(String groupId) {
		m_ackHolders.remove(new Pair<Boolean, String>(true, groupId));
		m_ackHolders.remove(new Pair<Boolean, String>(false, groupId));
		m_resendAckHolders.remove(new Pair<Boolean, String>(true, groupId));
		m_resendAckHolders.remove(new Pair<Boolean, String>(false, groupId));
		m_forwardOnlyAckHolders.remove(new Pair<Boolean, String>(true, groupId));
		m_forwardOnlyAckHolders.remove(new Pair<Boolean, String>(false, groupId));
	}

	@Override
	public void stop() {
		if (m_stopped.compareAndSet(false, true)) {
			// TODO remove legacy code
			for (AtomicReference<MessageQueueCursor> cursorRef : m_cursors.values()) {
				MessageQueueCursor cursor = cursorRef.get();
				if (cursor != null) {
					cursor.stop();
				}
			}

			m_ackOpTaskHandler.run();

			m_ackMsgsTaskHandler.run();

			doStop();
		}
	}

	@Override
	public void checkHolders() {
		for (Entry<Pair<Boolean, String>, AckHolder<MessageMeta>> entry : m_forwardOnlyAckHolders.entrySet()) {
			BatchResult<MessageMeta> result = entry.getValue().scan();
			doCheckHolders(entry.getKey(), result, false);
		}

		for (Entry<Pair<Boolean, String>, AckHolder<MessageMeta>> entry : m_ackHolders.entrySet()) {
			BatchResult<MessageMeta> result = entry.getValue().scan();
			doCheckHolders(entry.getKey(), result, false);
		}

		for (Entry<Pair<Boolean, String>, AckHolder<MessageMeta>> entry : m_resendAckHolders.entrySet()) {
			BatchResult<MessageMeta> result = entry.getValue().scan();
			doCheckHolders(entry.getKey(), result, true);
		}
	}

	protected void doCheckHolders(Pair<Boolean, String> pg, BatchResult<MessageMeta> result, boolean isResend) {
		if (result != null) {
			Tpp tpp = new Tpp(m_topic, m_partition, pg.getKey());
			String groupId = pg.getValue();

			ContinuousRange doneRange = result.getDoneRange();
			EnumRange<MessageMeta> failRange = result.getFailRange();
			if (failRange != null) {
				if (log.isDebugEnabled()) {
					log.debug(
					      "Nack messages(topic={}, partition={}, priority={}, groupId={}, isResend={}, msgIdToRemainingRetries={}).",
					      tpp.getTopic(), tpp.getPartition(), tpp.isPriority(), groupId, isResend, failRange.getOffsets());
				}

				try {
					doNack(isResend, pg.getKey(), groupId, failRange.getOffsets());
				} catch (Exception e) {
					log.error(
					      "Failed to nack messages(topic={}, partition={}, priority={}, groupId={}, isResend={}, msgIdToRemainingRetries={}).",
					      tpp.getTopic(), tpp.getPartition(), tpp.isPriority(), groupId, isResend, failRange.getOffsets(), e);
				}
			}

			if (doneRange != null) {
				if (log.isDebugEnabled()) {
					log.debug("Ack messages(topic={}, partition={}, priority={}, groupId={}, isResend={}, endOffset={}).",
					      tpp.getTopic(), tpp.getPartition(), tpp.isPriority(), groupId, isResend, doneRange.getEnd());
				}
				try {
					doAck(isResend, pg.getKey(), groupId, doneRange.getEnd());
				} catch (Exception e) {
					log.error("Ack messages(topic={}, partition={}, priority={}, groupId={}, isResend={}, endOffset={}).",
					      tpp.getTopic(), tpp.getPartition(), tpp.isPriority(), groupId, isResend, doneRange.getEnd(), e);
				}
			}
		}
	}

	private class AckMessagesTaskHandler implements Runnable {

		@Override
		public void run() {
			try {
				List<AckMessagesTask> todos = new ArrayList<>();
				m_ackMessageTaskQueue.drainTo(todos);

				for (AckMessagesTask todo : todos) {
					if (todo.getExpireTime() >= m_systemClockService.now()) {
						try {
							executeTask(todo);
						} catch (Exception e) {
							log.error("Exception occurred while executing ack message task.", e);
						}
					}
				}

			} finally {
				if (!m_stopped.get()) {
					m_ackMessagesTaskExecutor.schedule(m_ackMsgsTaskHandler,
					      m_config.getAckMessagesTaskExecutorCheckIntervalMillis(), TimeUnit.MILLISECONDS);
				}
			}
		}

		private void executeTask(AckMessagesTask task) {
			boolean success = false;
			try {
				doNackAndAck(task.getGroupId(), false, false, task.getAckedContexts(), task.getNackedContexts());
				doNackAndAck(task.getGroupId(), true, false, task.getAckedPriorityContexts(),
				      task.getNackedPriorityContexts());
				doNackAndAck(task.getGroupId(), false, true, task.getAckedResendContexts(), task.getNackedResendContexts());
				success = true;
			} finally {
				Command resCmd = null;

				switch (task.getCmdVersion()) {
				case 5:
					resCmd = new AckMessageResultCommandV5();
					((AckMessageResultCommandV5) resCmd).setSuccess(success);
					break;
				case 3:
				case 4:
				default:
					resCmd = new AckMessageResultCommandV3();
					((AckMessageResultCommandV3) resCmd).setSuccess(success);
					break;
				}
				resCmd.getHeader().setCorrelationId(task.getCorrelationId());
				ChannelUtils.writeAndFlush(task.getChannel(), resCmd);
			}
		}

		private void doNackAndAck(String groupId, boolean isPriority, boolean isResend, List<AckContext> ackedContexts,
		      List<AckContext> nackedContexts) {
			long maxAckOffset = calMaxAckOffset(ackedContexts, nackedContexts);

			if (CollectionUtil.isNotEmpty(nackedContexts)) {
				List<Pair<Long, MessageMeta>> msgId2Metas = new ArrayList<>(nackedContexts.size());
				for (AckContext ackContext : nackedContexts) {
					msgId2Metas.add(new Pair<>(ackContext.getMsgSeq(), new MessageMeta(ackContext.getMsgSeq(), ackContext
					      .getRemainingRetries(), -1L, isPriority ? 0 : 1, isResend)));
				}
				doNack(isResend, isPriority, groupId, msgId2Metas);
			}

			if (maxAckOffset >= 0) {
				doAck(isResend, isPriority, groupId, maxAckOffset);
			}
		}

		private long calMaxAckOffset(List<AckContext> ackedContexts, List<AckContext> nackedContexts) {
			long maxAckOffset = -1L;
			if (CollectionUtil.isNotEmpty(ackedContexts)) {
				for (AckContext context : ackedContexts) {
					if (context.getMsgSeq() > maxAckOffset) {
						maxAckOffset = context.getMsgSeq();
					}
				}
			}
			if (CollectionUtil.isNotEmpty(nackedContexts)) {
				for (AckContext context : nackedContexts) {
					if (context.getMsgSeq() > maxAckOffset) {
						maxAckOffset = context.getMsgSeq();
					}
				}
			}

			return maxAckOffset;
		}

	}

	private class AckOpTaskHandler implements Runnable {
		private List<Operation> m_todos = new ArrayList<Operation>();

		@Override
		public synchronized void run() {
			try {
				handleOperations();
				checkHolders();
			} catch (Exception e) {
				log.error("Exception occurred while executing ack task.", e);
			} finally {
				if (!m_stopped.get()) {
					m_ackOpExecutor.schedule(m_ackOpTaskHandler, m_config.getAckOpCheckIntervalMillis(),
					      TimeUnit.MILLISECONDS);
				}
			}
		}

		@SuppressWarnings("unchecked")
		private void handleOperations() {
			try {
				if (m_todos.isEmpty()) {
					m_opQueue.drainTo(m_todos, m_config.getAckOpHandlingBatchSize());
				}

				if (m_todos.isEmpty()) {
					return;
				}

				for (Operation op : m_todos) {
					AckHolder<MessageMeta> holder = findHolder(op);

					switch (op.getType()) {
					case ACK:
						holder.acked((Long) op.getData(), true);
						break;
					case NACK:
						holder.acked((Long) op.getData(), false);
						break;
					case DELIVERED:
						holder.delivered((List<Pair<Long, MessageMeta>>) op.getData(), op.getCreateTime());
						break;

					default:
						break;
					}
				}

				m_todos.clear();
			} catch (Exception e) {
				log.error("Exception occurred while handling operations.", e);
			}
		}

		private AckHolder<MessageMeta> findHolder(Operation op) {
			Map<Pair<Boolean, String>, AckHolder<MessageMeta>> holders = findHolders(op);

			AckHolder<MessageMeta> holder = null;
			holder = holders.get(op.getKey());
			if (holder == null) {
				int timeout = m_metaService.getAckTimeoutSecondsByTopicAndConsumerGroup(m_topic, op.getKey().getValue()) * 1000;

				holder = isForwordOnly(op) ? new ForwardOnlyAckHolder() : new DefaultAckHolder<MessageMeta>(timeout);
				holders.put(op.getKey(), holder);
			}

			return holder;
		}

		private Map<Pair<Boolean, String>, AckHolder<MessageMeta>> findHolders(Operation op) {
			return isForwordOnly(op) ? m_forwardOnlyAckHolders : (op.isResend() ? m_resendAckHolders : m_ackHolders);
		}

		private boolean isForwordOnly(Operation op) {
			return AckHolderType.FORWARD_ONLY == op.getAckHolderType();
		}
	}

	@Override
	public boolean offerAckHolderOp(Operation operation) {
		return m_opQueue.offer(operation);
	}

	@Override
	public boolean offerAckMessagesTask(AckMessagesTask task) {
		return m_ackMessageTaskQueue.offer(task);
	}

	protected abstract void doStop();

	protected abstract MessageQueueCursor create(String groupId, Lease lease, Offset offset);

	protected abstract void doNack(boolean resend, boolean isPriority, String groupId,
	      List<Pair<Long, MessageMeta>> msgId2Metas);

	protected abstract void doAck(boolean resend, boolean isPriority, String groupId, long msgSeq);
}
