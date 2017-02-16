package com.ctrip.hermes.broker.queue;

import java.lang.reflect.InvocationTargetException;
import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.DalRuntimeException;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.broker.biz.logger.BrokerFileBizLogger;
import com.ctrip.hermes.broker.queue.storage.MessageQueueStorage;
import com.ctrip.hermes.core.bo.SendMessageResult;
import com.ctrip.hermes.core.constants.CatConstants;
import com.ctrip.hermes.core.log.BizEvent;
import com.ctrip.hermes.core.message.PartialDecodedMessage;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.transport.command.MessageBatchWithRawData;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Storage;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Event;
import com.dianping.cat.message.Transaction;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.mchange.v2.resourcepool.TimeoutException;
import com.mysql.jdbc.PacketTooBigException;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class DefaultMessageQueueFlusher implements MessageQueueFlusher {

	private static final Logger log = LoggerFactory.getLogger(DefaultMessageQueueFlusher.class);

	private static final Logger skipLog = LoggerFactory.getLogger("MessageSkip");

	private static FutureBatchResultWrapperTransformer m_futureBatchResultWrapperTransformer = new FutureBatchResultWrapperTransformer();

	private static MessageBatchAndResultTransformer m_messageBatchAndResultTransformer = new MessageBatchAndResultTransformer();

	protected BrokerFileBizLogger m_bizLogger;

	private String m_topic;

	private int m_partition;

	private MessageQueueStorage m_storage;

	private BlockingQueue<PendingMessageWrapper> m_pendingMessages = new LinkedBlockingQueue<>();

	private MetaService m_metaService;

	public DefaultMessageQueueFlusher(String topic, int partition, MessageQueueStorage storage, MetaService metaService) {
		m_topic = topic;
		m_partition = partition;
		m_storage = storage;
		m_bizLogger = PlexusComponentLocator.lookup(BrokerFileBizLogger.class);
		m_metaService = metaService;
	}

	@Override
	public boolean hasUnflushedMessages() {
		return !m_pendingMessages.isEmpty();
	}

	@Override
	public long flush(int maxMsgCount) {
		long maxPurgedSelectorOffset = purgeExpiredMsgs();
		long maxSavedSelectorOffset = Long.MIN_VALUE;

		int msgCount = 0;

		List<PendingMessageWrapper> todos = new ArrayList<>();
		if (!m_pendingMessages.isEmpty()) {
			PendingMessageWrapper msg = m_pendingMessages.poll();
			todos.add(msg);
			msgCount = msg.getBatch().getMsgSeqs().size();

			PendingMessageWrapper nextMsg = null;
			while ((nextMsg = m_pendingMessages.peek()) != null) {
				int nextPendingMsgCount = nextMsg.getBatch().getMsgSeqs().size();
				if (msgCount + nextPendingMsgCount > maxMsgCount) {
					break;
				}
				todos.add(m_pendingMessages.poll());
				msgCount += nextPendingMsgCount;
			}
		}

		if (!todos.isEmpty()) {
			Transaction catTx = Cat.newTransaction(CatConstants.TYPE_MESSAGE_BROKER_FLUSH, m_topic + "-" + m_partition);
			catTx.addData("count", msgCount);

			maxSavedSelectorOffset = appendMessageSync(todos);

			catTx.setStatus(Transaction.SUCCESS);
			catTx.complete();
		}

		return Math.max(maxPurgedSelectorOffset, maxSavedSelectorOffset);
	}

	private long purgeExpiredMsgs() {
		long maxPurgedSelectorOffset = Long.MIN_VALUE;
		long now = System.currentTimeMillis();

		long purgedCount = 0;

		while (!m_pendingMessages.isEmpty()) {
			if (m_pendingMessages.peek().getExpireTime() < now) {
				long purgedSelectorOfset = purgeExpiredMsg();
				maxPurgedSelectorOffset = Math.max(maxPurgedSelectorOffset, purgedSelectorOfset);
				purgedCount++;
			} else {
				break;
			}
		}

		if (purgedCount > 0) {
			Cat.logEvent(CatConstants.TYPE_MESSAGE_PRODUCE_QUEUE_EXPIRED, m_topic, Event.SUCCESS, "*count=" + purgedCount);
		}

		return maxPurgedSelectorOffset;
	}

	private long purgeExpiredMsg() {
		PendingMessageWrapper messageWrapper = m_pendingMessages.poll();
		if (messageWrapper != null && messageWrapper.getBatch() != null) {
			Map<Integer, SendMessageResult> result = new HashMap<>();
			addResults(result, messageWrapper.getBatch().getMsgSeqs(), false);
			messageWrapper.getFuture().set(result);
		}
		return messageWrapper.getBatch().getSelectorOffset();
	}

	private static class FutureBatchResultWrapperTransformer implements
	      Function<FutureBatchResultWrapper, Pair<MessageBatchWithRawData, Map<Integer, SendMessageResult>>> {
		@Override
		public Pair<MessageBatchWithRawData, Map<Integer, SendMessageResult>> apply(FutureBatchResultWrapper input) {
			return new Pair<MessageBatchWithRawData, Map<Integer, SendMessageResult>>(input.getBatch(), input.getResult());
		}
	}

	protected long appendMessageSync(List<PendingMessageWrapper> todos) {
		long maxSelectorOffset = Long.MIN_VALUE;

		List<FutureBatchResultWrapper> priorityTodos = new ArrayList<>(todos.size());
		List<FutureBatchResultWrapper> nonPriorityTodos = new ArrayList<>(todos.size());

		for (PendingMessageWrapper todo : todos) {
			Map<Integer, SendMessageResult> result = new HashMap<>();
			maxSelectorOffset = Math.max(maxSelectorOffset, todo.getBatch().getSelectorOffset());
			addResults(result, todo.getBatch().getMsgSeqs(), false);

			if (todo.isPriority()) {
				priorityTodos.add(new FutureBatchResultWrapper(todo.getFuture(), todo.getBatch(), result));
			} else {
				nonPriorityTodos.add(new FutureBatchResultWrapper(todo.getFuture(), todo.getBatch(), result));
			}
		}

		doAppendMessageSync(true, Collections2.transform(priorityTodos, m_futureBatchResultWrapperTransformer));

		doAppendMessageSync(false, Collections2.transform(nonPriorityTodos, m_futureBatchResultWrapperTransformer));

		for (List<FutureBatchResultWrapper> todo : Arrays.asList(priorityTodos, nonPriorityTodos)) {
			for (FutureBatchResultWrapper fbw : todo) {
				SettableFuture<Map<Integer, SendMessageResult>> future = fbw.getFuture();
				Map<Integer, SendMessageResult> result = fbw.getResult();
				future.set(result);
			}
		}

		return maxSelectorOffset;
	}

	protected void addResults(Map<Integer, SendMessageResult> result, List<Integer> seqs, boolean success) {
		for (Integer seq : seqs) {
			result.put(seq, new SendMessageResult(success, false, null));
		}
	}

	protected void addResults(Map<Integer, SendMessageResult> result, boolean success, boolean shouldSkip,
	      String errorMessage, boolean shouldResponse) {
		for (Integer key : result.keySet()) {
			result.put(key, new SendMessageResult(success, shouldSkip, errorMessage, shouldResponse));
		}
	}

	@Override
	public ListenableFuture<Map<Integer, SendMessageResult>> append(boolean isPriority, MessageBatchWithRawData batch,
	      long expireTime) {
		SettableFuture<Map<Integer, SendMessageResult>> future = SettableFuture.create();

		m_pendingMessages.offer(new PendingMessageWrapper(future, batch, isPriority, expireTime));

		return future;
	}

	private static class MessageBatchAndResultTransformer implements
	      Function<Pair<MessageBatchWithRawData, Map<Integer, SendMessageResult>>, MessageBatchWithRawData> {
		@Override
		public MessageBatchWithRawData apply(Pair<MessageBatchWithRawData, Map<Integer, SendMessageResult>> input) {
			return input.getKey();
		}
	}

	protected void doAppendMessageSync(boolean isPriority,
	      Collection<Pair<MessageBatchWithRawData, Map<Integer, SendMessageResult>>> todos) {
		Collection<MessageBatchWithRawData> batches = null;
		try {
			batches = Collections2.transform(todos, m_messageBatchAndResultTransformer);
			m_storage.appendMessages(m_topic, m_partition, isPriority, batches);

			setBatchesResult(isPriority, todos, true, false, null, true);
		} catch (Exception e) {
			if (shoudlSkip(e)) {
				setBatchesResult(isPriority, todos, false, true, "Message too large.", true);
				if (batches != null) {
					for (MessageBatchWithRawData batch : batches) {
						for (PartialDecodedMessage msg : batch.getMessages()) {
							skipLog
							      .info("Message too large: {} {}\n{}", m_topic, m_partition, Arrays.toString(msg.readBody()));
						}
					}
				}
			} else if (isCheckoutTimeout(e)) {
				setBatchesResult(isPriority, todos, false, false, null, false);
				log.error("Failed to append messages due to DB checkout timeout!", e);
			} else {
				setBatchesResult(isPriority, todos, false, false, null, true);
				log.error("Failed to append messages.", e);
			}
		}
	}

	private boolean isCheckoutTimeout(Exception e) {
		if (e instanceof InvocationTargetException) {
			InvocationTargetException ite = (InvocationTargetException) e;
			if (ite.getTargetException() instanceof DalException) {
				DalException de = (DalException) ite.getTargetException();
				if (de.getCause() instanceof DalRuntimeException) {
					DalRuntimeException dre = (DalRuntimeException) de.getCause();
					if (dre.getCause() instanceof SQLException) {
						SQLException se = (SQLException) dre.getCause();
						if (se.getCause() instanceof TimeoutException) {
							return true;
						}
					}
				}
			}
		}
		return false;
	}

	private boolean shoudlSkip(Exception e) {
		if (e instanceof InvocationTargetException) {
			InvocationTargetException ite = (InvocationTargetException) e;
			if (ite.getTargetException() instanceof DalException) {
				DalException de = (DalException) ite.getTargetException();
				if (de.getCause() instanceof BatchUpdateException) {
					BatchUpdateException bue = (BatchUpdateException) de.getCause();
					if (bue.getCause() instanceof PacketTooBigException) {
						return true;
					}
				}
			}
		}

		return false;
	}

	private void setBatchesResult(boolean isPriority,
	      Collection<Pair<MessageBatchWithRawData, Map<Integer, SendMessageResult>>> todos, boolean success,
	      boolean shouldSkip, String errorMessage, boolean shouldResponse) {
		for (Pair<MessageBatchWithRawData, Map<Integer, SendMessageResult>> todo : todos) {
			bizLog(isPriority, todo.getKey(), success);
			Map<Integer, SendMessageResult> result = todo.getValue();
			addResults(result, success, shouldSkip, errorMessage, shouldResponse);
		}
	}

	private void bizLog(boolean isPriority, MessageBatchWithRawData batch, boolean success) {
		if (!Storage.KAFKA.equals(m_metaService.findTopicByName(batch.getTopic()).getStorageType())) {
			for (PartialDecodedMessage msg : batch.getMessages()) {
				BizEvent event = new BizEvent("Message.Saved");
				event.addData("topic", m_metaService.findTopicByName(batch.getTopic()).getId());
				event.addData("partition", m_partition);
				event.addData("priority", isPriority ? 0 : 1);
				event.addData("refKey", msg.getKey());
				event.addData("success", success);

				m_bizLogger.log(event);
			}
		}
	}

	private static class FutureBatchResultWrapper {
		private SettableFuture<Map<Integer, SendMessageResult>> m_future;

		private MessageBatchWithRawData m_batch;

		private Map<Integer, SendMessageResult> m_result;

		public FutureBatchResultWrapper(SettableFuture<Map<Integer, SendMessageResult>> future,
		      MessageBatchWithRawData batch, Map<Integer, SendMessageResult> result) {
			m_future = future;
			m_batch = batch;
			m_result = result;
		}

		public SettableFuture<Map<Integer, SendMessageResult>> getFuture() {
			return m_future;
		}

		public MessageBatchWithRawData getBatch() {
			return m_batch;
		}

		public Map<Integer, SendMessageResult> getResult() {
			return m_result;
		}

	}

	private static class PendingMessageWrapper {
		private SettableFuture<Map<Integer, SendMessageResult>> m_future;

		private MessageBatchWithRawData m_batch;

		private boolean m_isPriority;

		private long m_expireTime;

		public PendingMessageWrapper(SettableFuture<Map<Integer, SendMessageResult>> future,
		      MessageBatchWithRawData batch, boolean isPriority, long expireTime) {
			m_future = future;
			m_batch = batch;
			m_isPriority = isPriority;
			m_expireTime = expireTime;
		}

		public SettableFuture<Map<Integer, SendMessageResult>> getFuture() {
			return m_future;
		}

		public MessageBatchWithRawData getBatch() {
			return m_batch;
		}

		public boolean isPriority() {
			return m_isPriority;
		}

		public long getExpireTime() {
			return m_expireTime;
		}

	}

}
