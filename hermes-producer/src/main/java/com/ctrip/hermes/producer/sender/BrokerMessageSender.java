package com.ctrip.hermes.producer.sender;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.bo.SendMessageResult;
import com.ctrip.hermes.core.constants.CatConstants;
import com.ctrip.hermes.core.constants.IdcPolicy;
import com.ctrip.hermes.core.exception.MessageSendException;
import com.ctrip.hermes.core.kafka.KafkaIdcStrategy;
import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.core.selector.CallbackContext;
import com.ctrip.hermes.core.selector.DefaultOffsetGenerator;
import com.ctrip.hermes.core.selector.FixedExpireTimeHolder;
import com.ctrip.hermes.core.selector.OffsetGenerator;
import com.ctrip.hermes.core.selector.SelectorCallback;
import com.ctrip.hermes.core.selector.Slot;
import com.ctrip.hermes.core.selector.TriggerResult;
import com.ctrip.hermes.core.selector.TriggerResult.State;
import com.ctrip.hermes.core.transport.command.v6.SendMessageCommandV6;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.producer.config.ProducerConfig;
import com.ctrip.hermes.producer.status.ProducerStatusMonitor;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = MessageSender.class, value = Endpoint.BROKER)
public class BrokerMessageSender extends AbstractMessageSender implements MessageSender {

	private static Logger log = LoggerFactory.getLogger(BrokerMessageSender.class);

	@Inject
	private ProducerConfig m_config;

	@Inject
	private MessageSenderSelectorManager m_selectorManager;

	@Inject
	private KafkaIdcStrategy m_kafkaIdcStrategy;

	private ConcurrentMap<Pair<String, Integer>, TaskQueue> m_taskQueues = new ConcurrentHashMap<Pair<String, Integer>, TaskQueue>();

	private ExecutorService m_callbackExecutor;

	private AtomicBoolean m_started = new AtomicBoolean(false);

	@Override
	public Future<SendResult> doSend(ProducerMessage<?> msg) {
		if (m_started.compareAndSet(false, true)) {
			int callbackThreadCount = m_config.getProducerCallbackThreadCount();

			m_callbackExecutor = Executors.newFixedThreadPool(callbackThreadCount,
			      HermesThreadFactory.create("ProducerCallback", false));
		}

		final Pair<String, Integer> taskQueueKey = computeTaskQueueKey(msg);
		TaskQueue taskQueue = getOrInitializeTaskQueue(taskQueueKey, msg);

		Future<SendResult> f;
		// generate offset and put to "wait queue" should be atomic
		synchronized (taskQueue) {
			msg.setSelectorOffset(taskQueue.nextOffset());
			f = taskQueue.submit(msg);
		}

		m_selectorManager.getSelector().update(taskQueueKey, true, new Slot(0, msg.getSelectorOffset()));
		return f;
	}

	private TaskQueue getOrInitializeTaskQueue(final Pair<String, Integer> taskQueueKey, ProducerMessage<?> msg) {
		TaskQueue taskQueue = m_taskQueues.get(taskQueueKey);

		if (taskQueue == null) {
			synchronized (this) {
				taskQueue = m_taskQueues.get(taskQueueKey);

				if (taskQueue == null) {
					taskQueue = new TaskQueue(msg.getTopic(), msg.getPartition(), m_config.getBrokerSenderTaskQueueSize());
					m_taskQueues.put(taskQueueKey, taskQueue);

					m_selectorManager.register(taskQueueKey, new FixedExpireTimeHolder(Long.MAX_VALUE),
					      new SelectorCallback() {

						      @Override
						      public void onReady(CallbackContext ctx) {
							      new SendTask(taskQueueKey, ctx).send();
						      }
					      }, null, taskQueue.nextOffset());
				}
			}
		}

		return taskQueue;
	}

	private Pair<String, Integer> computeTaskQueueKey(ProducerMessage<?> msg) {
		int pkHashCode = getPartitionKeyHashCode(msg);

		int taskQueueIndex = msg.getPartition()
		      * m_config.getBrokerSenderConcurrentLevel()
		      + ((pkHashCode == Integer.MIN_VALUE ? 0 : Math.abs(pkHashCode)) % m_config.getBrokerSenderConcurrentLevel());

		Pair<String, Integer> taskQueueKey = new Pair<String, Integer>(msg.getTopic(), taskQueueIndex);
		return taskQueueKey;
	}

	private class SendTask {

		private Pair<String, Integer> taskQueueKey;

		private CallbackContext callbackContext;

		public SendTask(Pair<String, Integer> taskQueueKey, CallbackContext callbackContext) {
			this.taskQueueKey = taskQueueKey;
			this.callbackContext = callbackContext;
		}

		public void send() {
			boolean selectorSendSuccess = false;
			long cmdSelectorOffset = 0;
			SendMessageCommandV6 cmd = null;
			int batchSize = m_config.getBrokerSenderBatchSize();

			try {
				TaskQueue taskQueue = m_taskQueues.get(taskQueueKey);
				cmd = taskQueue.pop(batchSize);

				if (cmd != null) {
					selectorSendSuccess = true;
					cmdSelectorOffset = cmd.getSelectorOffset();

					if (!cmd.getProducerMessages().isEmpty()) {

						int produceTimeoutSeconds = m_config.getProduceTimeoutSeconds(cmd.getTopic());

						if (produceTimeoutSeconds > 0
						      && System.currentTimeMillis() - cmd.getBornTime() > produceTimeoutSeconds * 1000) {
							for (Pair<SettableFuture<SendResult>, ProducerMessage<?>> pair : cmd.getFutureAndMessagePair()) {
								MessageSendException exception = new MessageSendException("Send timeout.", pair.getValue());
								notifySendFail(pair.getKey(), exception);
							}

							Transaction t = Cat.newTransaction(CatConstants.TYPE_MESSAGE_PRODUCE_TIMEOUT, cmd.getTopic());
							t.addData("*count", cmd.getMessageCount());
							t.setStatus(Transaction.SUCCESS);
							t.complete();
						} else {
							SendMessageResult sendResult = sendMessagesToBroker(cmd);

							if (!sendResult.isSuccess() && !sendResult.isShouldSkip()) {
								selectorSendSuccess = false;
								taskQueue.push(cmd);
							}
							tracking(cmd, sendResult);
						}
					}
				}
			} catch (Exception e) {
				log.error("Unexpected exception when send cmd to broker", e);
				selectorSendSuccess = false;
			} finally {

				State state;
				if (selectorSendSuccess) {
					state = State.GotAndSuccessfullyProcessed;
				} else {
					if (cmd == null) {
						state = State.GotNothing;
					} else {
						state = State.GotButErrorInProcessing;
					}
				}
				TriggerResult triggerResult = new TriggerResult(state, new long[] { cmdSelectorOffset });

				m_selectorManager.reRegister(taskQueueKey, callbackContext, triggerResult, new FixedExpireTimeHolder(
				      Long.MAX_VALUE), new SelectorCallback() {

					@Override
					public void onReady(CallbackContext innerCtx) {
						new SendTask(taskQueueKey, innerCtx).send();
					}
				});
			}
		}

		private void tracking(SendMessageCommandV6 sendMessageCommand, SendMessageResult sendResult) {
			if (!sendResult.isSuccess()) {
				if (!sendResult.isShouldSkip()) {
					Transaction t = Cat.newTransaction(CatConstants.TYPE_MESSAGE_PRODUCE_TRANSPORT,
					      sendMessageCommand.getTopic());
					t.addData("*count", sendMessageCommand.getMessageCount());
					t.setStatus("FAILED");
					t.complete();
				} else {
					Transaction t = Cat.newTransaction(CatConstants.TYPE_MESSAGE_PRODUCE_TRANSPORT_SKIP,
					      sendMessageCommand.getTopic());
					t.addData("*count", sendMessageCommand.getMessageCount());
					t.setStatus(Transaction.SUCCESS);
					t.complete();
				}
			} else {
				if (m_config.isCatEnabled()) {
					Transaction t = Cat.newTransaction(CatConstants.TYPE_MESSAGE_PRODUCE_TRANSPORT,
					      sendMessageCommand.getTopic());
					t.addData("*count", sendMessageCommand.getMessageCount());
					t.setStatus(Transaction.SUCCESS);
					t.complete();
				}
			}

		}

		private SendMessageResult sendMessagesToBroker(SendMessageCommandV6 cmd) {
			String topic = cmd.getTopic();
			int partition = cmd.getPartition();
			long correlationId = cmd.getHeader().getCorrelationId();
			try {
				Endpoint endpoint = m_endpointManager.getEndpoint(topic, partition);
				if (endpoint != null) {
					Future<Pair<Boolean, Endpoint>> acceptFuture = m_messageAcceptanceMonitor.monitor(correlationId);
					Future<SendMessageResult> resultFuture = m_messageResultMonitor.monitor(cmd);

					long acceptTimeout = m_config.getBrokerSenderAcceptTimeoutMillis();
					long resultTimeout = cmd.getTimeout();

					if (m_endpointClient.writeCommand(endpoint, cmd)) {
						Pair<Boolean, Endpoint> acceptResult = waitForBrokerAcceptance(cmd, acceptFuture, acceptTimeout);

						if (acceptResult != null) {
							return waitForBrokerResultIfNecessary(cmd, resultFuture, acceptResult, resultTimeout);
						} else {
							m_endpointManager.refreshEndpoint(topic, partition);
							return new SendMessageResult(false, false, null);
						}
					} else {
						m_endpointManager.refreshEndpoint(topic, partition);
						return new SendMessageResult(false, false, null);
					}
				} else {
					// ignore
					if (log.isDebugEnabled()) {
						log.debug("No endpoint found, ignore it");
					}
					m_endpointManager.refreshEndpoint(topic, partition);
					return new SendMessageResult(false, false, null);
				}
			} catch (Exception e) {
				log.warn("Exception occurred while sending message to broker, will retry it");
				return new SendMessageResult(false, false, null);
			} finally {
				m_messageAcceptanceMonitor.cancel(correlationId);
				m_messageResultMonitor.cancel(cmd);
			}
		}

		private Pair<Boolean, Endpoint> waitForBrokerAcceptance(SendMessageCommandV6 cmd,
		      Future<Pair<Boolean, Endpoint>> acceptFuture, long timeout) throws InterruptedException, ExecutionException {
			Pair<Boolean, Endpoint> acceptResult = null;
			try {
				acceptResult = acceptFuture.get(timeout, TimeUnit.MILLISECONDS);
			} catch (TimeoutException e) {
				ProducerStatusMonitor.INSTANCE.brokerAcceptTimeout(cmd.getTopic(), cmd.getPartition(),
				      cmd.getMessageCount());
			}
			return acceptResult;
		}

		private SendMessageResult waitForBrokerResultIfNecessary(SendMessageCommandV6 cmd,
		      Future<SendMessageResult> resultFuture, Pair<Boolean, Endpoint> acceptResult, long timeout)
		      throws InterruptedException, ExecutionException {
			Boolean brokerAccept = acceptResult.getKey();
			String topic = cmd.getTopic();
			int partition = cmd.getPartition();
			if (brokerAccept != null && brokerAccept) {
				return waitForBrokerResult(cmd, resultFuture, timeout);
			} else {
				ProducerStatusMonitor.INSTANCE.brokerRejected(topic, partition, cmd.getMessageCount());
				Endpoint newEndpoint = acceptResult.getValue();
				if (newEndpoint != null) {
					m_endpointManager.updateEndpoint(topic, partition, newEndpoint);
				}
				return new SendMessageResult(false, false, null);
			}
		}

		private SendMessageResult waitForBrokerResult(SendMessageCommandV6 cmd, Future<SendMessageResult> resultFuture,
		      long timeout) throws InterruptedException, ExecutionException {
			try {
				return resultFuture.get(timeout, TimeUnit.MILLISECONDS);
			} catch (TimeoutException e) {
				// ignore
			}

			return new SendMessageResult(false, false, null);
		}
	}

	private void notifySendFail(SettableFuture<SendResult> future, MessageSendException exception) {
		future.setException(exception);
	}

	private class TaskQueue implements OffsetGenerator {
		private String m_topic;

		private int m_partition;

		private AtomicReference<SendMessageCommandV6> m_cmd = new AtomicReference<>(null);

		private BlockingQueue<ProducerWorkerContext> m_queue;

		private OffsetGenerator m_offsetGenerator = new DefaultOffsetGenerator();

		public TaskQueue(String topic, int partition, int queueSize) {
			m_topic = topic;
			m_partition = partition;
			m_queue = new ArrayBlockingQueue<ProducerWorkerContext>(queueSize);
			ProducerStatusMonitor.INSTANCE.watchTaskQueue(m_topic, m_partition, m_queue);
		}

		public long nextOffset() {
			return m_offsetGenerator.nextOffset(1);
		}

		public void push(SendMessageCommandV6 cmd) {
			m_cmd.set(cmd);
		}

		public SendMessageCommandV6 pop(int size) {
			if (m_cmd.get() == null) {
				return createSendMessageCommand(size);
			}
			return m_cmd.getAndSet(null);
		}

		private SendMessageCommandV6 createSendMessageCommand(int size) {
			long maxMsgSelectorOffset = Long.MIN_VALUE;
			SendMessageCommandV6 cmd = null;
			List<ProducerWorkerContext> contexts = new ArrayList<ProducerWorkerContext>(size);
			m_queue.drainTo(contexts, size);
			if (!contexts.isEmpty()) {
				cmd = new SendMessageCommandV6(m_topic, m_partition, m_config.getBrokerSenderResultTimeoutMillis());
				for (ProducerWorkerContext context : contexts) {
					int produceTimeoutSeconds = m_config.getProduceTimeoutSeconds(m_topic);

					if (produceTimeoutSeconds > 0
					      && System.currentTimeMillis() - context.m_msg.getBornTime() > produceTimeoutSeconds * 1000) {
						MessageSendException exception = new MessageSendException("Send timeout.", context.m_msg);
						notifySendFail(context.m_future, exception);

						Transaction t = Cat.newTransaction(CatConstants.TYPE_MESSAGE_PRODUCE_TIMEOUT, m_topic);
						t.setStatus(Transaction.SUCCESS);
						t.complete();
					} else {
						cmd.addMessage(context.m_msg, context.m_future);
					}
					maxMsgSelectorOffset = Math.max(maxMsgSelectorOffset, context.m_msg.getSelectorOffset());
				}
				cmd.setSelectorOffset(maxMsgSelectorOffset);
				cmd.setTargetIdc(getTargetIdc().toLowerCase());
			}
			return cmd;
		}

		private String getTargetIdc() {
			String idcPolicy = m_metaService.findTopicByName(m_topic).getIdcPolicy();
			if (idcPolicy == null) {
				idcPolicy = IdcPolicy.PRIMARY;
			}

			String targetIdc = m_kafkaIdcStrategy.getTargetIdc(idcPolicy);
			if (targetIdc == null) {
				throw new RuntimeException("Can not get target idc!");
			}

			return targetIdc;
		}

		public Future<SendResult> submit(final ProducerMessage<?> msg) {
			SettableFuture<SendResult> future = SettableFuture.create();

			if (msg.getCallback() != null) {
				Futures.addCallback(future, new FutureCallback<SendResult>() {

					@Override
					public void onSuccess(SendResult result) {
						msg.getCallback().onSuccess(result);
					}

					@Override
					public void onFailure(Throwable t) {
						msg.getCallback().onFailure(t);
					}
				}, m_callbackExecutor);
			}

			offer(msg, future);

			return future;
		}

		private void offer(final ProducerMessage<?> msg, SettableFuture<SendResult> future) {
			if (!m_queue.offer(new ProducerWorkerContext(msg, future))) {
				ProducerStatusMonitor.INSTANCE.offerFailed(m_topic, m_partition);
				String body = null;
				try {
					body = JSON.toJSONString(msg);
				} catch (Exception e) {
					body = msg.toString();
				}
				String warning = String.format(
				      "Producer task queue is full(queueSize=%s), will drop this message(refKey=%s, body=%s).",
				      m_queue.size(), msg.getKey(), body);
				log.warn(warning);
				MessageSendException throwable = new MessageSendException(warning, msg);
				notifySendFail(future, throwable);
			}
		}

		@Override
		public long nextOffset(int delta) {
			throw new UnsupportedOperationException();
		}
	}

	private static class ProducerWorkerContext {
		private ProducerMessage<?> m_msg;

		private SettableFuture<SendResult> m_future;

		public ProducerWorkerContext(ProducerMessage<?> msg, SettableFuture<SendResult> future) {
			m_msg = msg;
			m_future = future;
		}

	}

}
