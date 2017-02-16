package com.ctrip.hermes.consumer.engine.bootstrap.strategy;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.consumer.build.BuildConstants;
import com.ctrip.hermes.consumer.engine.ConsumerContext;
import com.ctrip.hermes.consumer.engine.FilterMessageListenerConfig;
import com.ctrip.hermes.consumer.engine.ack.AckManager;
import com.ctrip.hermes.consumer.engine.config.ConsumerConfig;
import com.ctrip.hermes.consumer.engine.lease.ConsumerLeaseKey;
import com.ctrip.hermes.consumer.engine.monitor.PullMessageAcceptanceMonitor;
import com.ctrip.hermes.consumer.engine.monitor.PullMessageResultMonitor;
import com.ctrip.hermes.consumer.engine.monitor.QueryOffsetAcceptanceMonitor;
import com.ctrip.hermes.consumer.engine.monitor.QueryOffsetResultMonitor;
import com.ctrip.hermes.consumer.engine.notifier.ConsumerNotifier;
import com.ctrip.hermes.consumer.engine.status.ConsumerStatusMonitor;
import com.ctrip.hermes.consumer.message.BrokerConsumerMessage;
import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;
import com.ctrip.hermes.core.lease.LeaseManager;
import com.ctrip.hermes.core.message.BaseConsumerMessage;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch.MessageMeta;
import com.ctrip.hermes.core.message.codec.MessageCodec;
import com.ctrip.hermes.core.message.retry.RetryPolicy;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.schedule.ExponentialSchedulePolicy;
import com.ctrip.hermes.core.schedule.SchedulePolicy;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.core.transport.command.CorrelationIdGenerator;
import com.ctrip.hermes.core.transport.command.v5.PullMessageCommandV5;
import com.ctrip.hermes.core.transport.command.v5.PullMessageResultCommandV5;
import com.ctrip.hermes.core.transport.command.v5.QueryLatestConsumerOffsetCommandV5;
import com.ctrip.hermes.core.transport.command.v5.QueryOffsetResultCommandV5;
import com.ctrip.hermes.core.transport.endpoint.EndpointClient;
import com.ctrip.hermes.core.transport.endpoint.EndpointManager;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public abstract class BaseConsumerTask implements ConsumerTask {

	private static final Logger log = LoggerFactory.getLogger(BaseConsumerTask.class);

	protected SystemClockService m_systemClockService;

	protected LeaseManager<ConsumerLeaseKey> m_leaseManager;

	protected ConsumerConfig m_config;

	protected ConsumerNotifier m_consumerNotifier;

	protected ExecutorService m_pullMessageTaskExecutor;

	protected ScheduledExecutorService m_renewLeaseTaskExecutor;

	protected BlockingQueue<ConsumerMessage<?>> m_msgs;

	protected ConsumerContext m_context;

	protected int m_partitionId;

	protected AtomicBoolean m_pullTaskRunning = new AtomicBoolean(false);

	protected AtomicReference<Lease> m_lease = new AtomicReference<Lease>(null);

	protected AtomicBoolean m_closed = new AtomicBoolean(false);

	protected RetryPolicy m_retryPolicy;

	protected AtomicReference<Runnable> m_pullMessagesTask = new AtomicReference<>(null);

	protected AtomicReference<AtomicReference<Offset>> m_offset = new AtomicReference<>();

	protected AckManager m_ackManager;

	protected int m_maxAckHolderSize;

	protected AtomicLong m_token = new AtomicLong(-1L);

	public BaseConsumerTask(ConsumerContext context, int partitionId, int localCacheSize, AckManager ackManager) {
		this(context, partitionId, localCacheSize, 0, ackManager);
	}

	public BaseConsumerTask(ConsumerContext context, int partitionId, int localCacheSize, int maxAckHolderSize) {
		this(context, partitionId, localCacheSize, maxAckHolderSize, PlexusComponentLocator.lookup(AckManager.class));
	}

	@SuppressWarnings("unchecked")
	private BaseConsumerTask(ConsumerContext context, int partitionId, int localCacheSize, int maxAckHolderSize,
	      AckManager ackManager) {
		m_context = context;
		m_partitionId = partitionId;
		m_msgs = new LinkedBlockingQueue<ConsumerMessage<?>>(localCacheSize);
		m_maxAckHolderSize = maxAckHolderSize;

		m_leaseManager = PlexusComponentLocator.lookup(LeaseManager.class, BuildConstants.CONSUMER);
		m_consumerNotifier = PlexusComponentLocator.lookup(ConsumerNotifier.class);
		m_systemClockService = PlexusComponentLocator.lookup(SystemClockService.class);
		m_config = PlexusComponentLocator.lookup(ConsumerConfig.class);
		m_retryPolicy = PlexusComponentLocator.lookup(MetaService.class).findRetryPolicyByTopicAndGroup(
		      context.getTopic().getName(), context.getGroupId());
		m_ackManager = ackManager;

		m_pullMessageTaskExecutor = Executors.newSingleThreadExecutor(HermesThreadFactory.create(
		      String.format("PullMessageThread-%s-%s-%s", m_context.getTopic().getName(), m_partitionId,
		            m_context.getGroupId()), false));

		m_renewLeaseTaskExecutor = Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create(
		      String.format("RenewLeaseThread-%s-%s-%s", m_context.getTopic().getName(), m_partitionId,
		            m_context.getGroupId()), false));

		ConsumerStatusMonitor.INSTANCE.watchLocalCache(m_context.getTopic().getName(), m_partitionId,
		      m_context.getGroupId(), m_msgs);
	}

	protected boolean isClosed() {
		return m_closed.get();
	}

	@Override
	public void start() {
		// FIXME one boss thread to acquire lease, if acquired, run this task
		log.info("Consumer started(mode={}, topic={}, partition={}, groupId={}, sessionId={})",
		      m_context.getConsumerType(), m_context.getTopic().getName(), m_partitionId, m_context.getGroupId(),
		      m_context.getSessionId());

		ConsumerLeaseKey key = new ConsumerLeaseKey(new Tpg(m_context.getTopic().getName(), m_partitionId,
		      m_context.getGroupId()), m_context.getSessionId());

		while (!isClosed() && !Thread.currentThread().isInterrupted()) {
			try {
				acquireLease(key);

				if (!isClosed() && m_lease.get() != null && !m_lease.get().isExpired()) {

					m_token.set(CorrelationIdGenerator.generateCorrelationId());

					log.info(
					      "Consumer continue consuming(mode={}, topic={}, partition={}, groupId={}, token={}, sessionId={}), since lease acquired",
					      m_context.getConsumerType(), m_context.getTopic().getName(), m_partitionId,
					      m_context.getGroupId(), m_token.get(), m_context.getSessionId());

					startConsuming(key, m_token.get());

					m_token.set(-1L);

					log.info(
					      "Consumer pause consuming(mode={}, topic={}, partition={}, groupId={}, token={}, sessionId={}), since lease expired",
					      m_context.getConsumerType(), m_context.getTopic().getName(), m_partitionId,
					      m_context.getGroupId(), m_token.get(), m_context.getSessionId());
				}
			} catch (Exception e) {
				log.error("Exception occurred in consumer's run method(topic={}, partition={}, groupId={}, sessionId={})",
				      m_context.getTopic().getName(), m_partitionId, m_context.getGroupId(), m_context.getSessionId(), e);
			}
		}

		stopConsumer();
	}

	protected void stopConsumer() {
		m_pullMessageTaskExecutor.shutdown();
		m_renewLeaseTaskExecutor.shutdown();

		ConsumerStatusMonitor.INSTANCE.stopWatchLocalCache(m_context.getTopic().getName(), m_partitionId,
		      m_context.getGroupId());

		log.info("Consumer stopped(mode={}, topic={}, partition={}, groupId={}, sessionId={})",
		      m_context.getConsumerType(), m_context.getTopic().getName(), m_partitionId, m_context.getGroupId(),
		      m_context.getSessionId());
	}

	protected void startConsuming(ConsumerLeaseKey key, long token) {
		m_consumerNotifier.register(token, m_context);
		m_ackManager.register(token, new Tpg(m_context.getTopic().getName(), m_partitionId, m_context.getGroupId()),
		      m_maxAckHolderSize);
		doBeforeConsuming(key, token);

		m_msgs.clear();

		SchedulePolicy noMessageSchedulePolicy = new ExponentialSchedulePolicy(m_config.getNoMessageWaitBaseMillis(),
		      m_config.getNoMessageWaitMaxMillis());

		while (!isClosed() && !Thread.currentThread().isInterrupted() && !m_lease.get().isExpired()) {

			try {
				// if leaseRemainingTime < stopConsumerTimeMillsBeforLeaseExpired, stop
				if (m_lease.get().getRemainingTime() <= m_config.getStopConsumerTimeMillsBeforLeaseExpired()) {
					log.info(
					      "Consumer pre-pause(topic={}, partition={}, groupId={}, token={}, sessionId={}), since lease will be expired soon",
					      m_context.getTopic().getName(), m_partitionId, m_context.getGroupId(), token,
					      m_context.getSessionId());
					break;
				}

				if (m_msgs.isEmpty()) {
					schedulePullMessagesTask();
				}

				if (!m_msgs.isEmpty()) {
					consumeMessages(token);
					noMessageSchedulePolicy.succeess();
				} else {
					noMessageSchedulePolicy.fail(true);
				}

			} catch (Exception e) {
				log.error("Exception occurred while consuming message(topic={}, partition={}, groupId={}, sessionId={})",
				      m_context.getTopic().getName(), m_partitionId, m_context.getGroupId(), m_context.getSessionId(), e);
			}
		}

		m_consumerNotifier.deregister(token, false);
		m_ackManager.deregister(token);
		m_lease.set(null);
		doAfterConsuming(key);
	}

	protected void schedulePullMessagesTask() {
		if (!isClosed() && m_pullTaskRunning.compareAndSet(false, true)) {
			m_pullMessageTaskExecutor.submit(getPullMessageTask());
		}
	}

	protected Runnable getPullMessageTask() {
		return m_pullMessagesTask.get();
	}

	protected Runnable createPullMessageTask( //
	      long token, AtomicReference<Offset> baseOffset, SchedulePolicy noEndpointSchedulePolicy) {
		return new BasePullMessagesTask(token, baseOffset, noEndpointSchedulePolicy);
	}

	protected void doBeforeConsuming(ConsumerLeaseKey key, long token) {
		queryLatestOffset(key);

		SchedulePolicy noEndpointSchedulePolicy = new ExponentialSchedulePolicy(m_config.getNoEndpointWaitBaseMillis(),
		      m_config.getNoEndpointWaitMaxMillis());
		m_pullMessagesTask.set(createPullMessageTask(token, m_offset.get(), noEndpointSchedulePolicy));
	}

	protected void queryLatestOffset(ConsumerLeaseKey key) {

		SchedulePolicy schedulePolicy = new ExponentialSchedulePolicy(m_config.getNoEndpointWaitBaseMillis(),
		      m_config.getNoEndpointWaitMaxMillis());

		EndpointManager endpointManager = PlexusComponentLocator.lookup(EndpointManager.class);
		while (!isClosed() && !Thread.currentThread().isInterrupted() && !m_lease.get().isExpired()) {
			Endpoint endpoint = endpointManager.getEndpoint(m_context.getTopic().getName(), m_partitionId);
			if (endpoint == null) {
				log.warn("No endpoint found for topic {} partition {}, will retry later", m_context.getTopic().getName(),
				      m_partitionId);
				endpointManager.refreshEndpoint(m_context.getTopic().getName(), m_partitionId);
				schedulePolicy.fail(true);
			} else {
				final SettableFuture<QueryOffsetResultCommandV5> resultFuture = SettableFuture.create();

				QueryLatestConsumerOffsetCommandV5 cmd = new QueryLatestConsumerOffsetCommandV5(m_context.getTopic()
				      .getName(), m_partitionId, m_context.getGroupId());

				cmd.setFuture(resultFuture);

				QueryOffsetResultCommandV5 offsetRes = null;

				QueryOffsetResultMonitor queryOffsetResultMonitor = PlexusComponentLocator
				      .lookup(QueryOffsetResultMonitor.class);
				QueryOffsetAcceptanceMonitor queryOffsetAcceptMonitor = PlexusComponentLocator
				      .lookup(QueryOffsetAcceptanceMonitor.class);

				long acceptTimeout = m_config.getQueryOffsetAcceptTimeoutMillis();

				try {
					queryOffsetResultMonitor.monitor(cmd);
					SettableFuture<Pair<Boolean, Endpoint>> acceptFuture = queryOffsetAcceptMonitor.monitor(cmd.getHeader()
					      .getCorrelationId());
					if (PlexusComponentLocator.lookup(EndpointClient.class).writeCommand(endpoint, cmd)) {

						Pair<Boolean, Endpoint> acceptResult = waitForBrokerAcceptance(acceptFuture, acceptTimeout);

						if (acceptResult != null) {
							offsetRes = waitForBrokerResultIfNecessary(cmd, resultFuture, acceptResult,
							      m_config.getQueryOffsetTimeoutMillis());
						} else {
							endpointManager.refreshEndpoint(cmd.getTopic(), cmd.getPartition());
						}

						if (offsetRes != null && offsetRes.getOffset() != null) {
							m_offset.set(new AtomicReference<Offset>(offsetRes.getOffset()));
							return;
						} else {
							schedulePolicy.fail(true);
						}

					} else {
						endpointManager.refreshEndpoint(cmd.getTopic(), cmd.getPartition());
						schedulePolicy.fail(true);
					}

				} catch (Exception e) {
					// ignore
				} finally {
					queryOffsetResultMonitor.remove(cmd);
					queryOffsetAcceptMonitor.cancel(cmd.getHeader().getCorrelationId());
				}

			}
		}
	}

	private Pair<Boolean, Endpoint> waitForBrokerAcceptance(Future<Pair<Boolean, Endpoint>> acceptFuture, long timeout)
	      throws InterruptedException, ExecutionException {
		Pair<Boolean, Endpoint> acceptResult = null;
		try {
			acceptResult = acceptFuture.get(timeout, TimeUnit.MILLISECONDS);
		} catch (Exception e) {
			// ignore
		}
		return acceptResult;
	}

	private QueryOffsetResultCommandV5 waitForBrokerResultIfNecessary(QueryLatestConsumerOffsetCommandV5 cmd,
	      SettableFuture<QueryOffsetResultCommandV5> resultFuture, Pair<Boolean, Endpoint> acceptResult, long timeout)
	      throws InterruptedException, ExecutionException {
		Boolean brokerAccept = acceptResult.getKey();
		String topic = cmd.getTopic();
		int partition = cmd.getPartition();
		if (brokerAccept != null && brokerAccept) {
			return waitForBrokerResult(resultFuture, timeout);
		} else {
			Endpoint newEndpoint = acceptResult.getValue();
			if (newEndpoint != null) {
				PlexusComponentLocator.lookup(EndpointManager.class).updateEndpoint(topic, partition, newEndpoint);
			}
			return null;
		}
	}

	private QueryOffsetResultCommandV5 waitForBrokerResult(Future<QueryOffsetResultCommandV5> resultFuture, long timeout)
	      throws InterruptedException, ExecutionException {
		QueryOffsetResultCommandV5 result = null;
		try {
			result = resultFuture.get(timeout, TimeUnit.MILLISECONDS);
		} catch (TimeoutException e) {
			// do nothing
		} catch (ExecutionException e) {
			// do nothing
		}

		return result;
	}

	protected void doAfterConsuming(ConsumerLeaseKey key) {
		m_pullMessagesTask.set(null);
		m_offset.set(null);
	}

	protected void scheduleRenewLeaseTask(final ConsumerLeaseKey key, long delay) {
		m_renewLeaseTaskExecutor.schedule(new Runnable() {

			@Override
			public void run() {
				if (isClosed()) {
					return;
				}

				Lease lease = m_lease.get();
				if (lease != null) {
					if (lease.getRemainingTime() > 0) {
						LeaseAcquireResponse response = m_leaseManager.tryRenewLease(key, lease);
						if (response != null && response.isAcquired()) {

							lease.setExpireTime(response.getLease().getExpireTime());
							scheduleRenewLeaseTask(key,
							      lease.getRemainingTime() - m_config.getRenewLeaseTimeMillisBeforeExpired());

							if (log.isDebugEnabled()) {
								log.debug("Consumer renew lease success(topic={}, partition={}, groupId={}, sessionId={})",
								      m_context.getTopic().getName(), m_partitionId, m_context.getGroupId(),
								      m_context.getSessionId());
							}
						} else {
							if (response != null && response.getNextTryTime() > 0) {
								scheduleRenewLeaseTask(key, response.getNextTryTime() - m_systemClockService.now());
							} else {
								scheduleRenewLeaseTask(key, m_config.getDefaultLeaseRenewDelayMillis());
							}

							if (log.isDebugEnabled()) {
								log.debug(
								      "Unable to renew consumer lease(topic={}, partition={}, groupId={}, sessionId={}), ignore it",
								      m_context.getTopic().getName(), m_partitionId, m_context.getGroupId(),
								      m_context.getSessionId());
							}
						}
					}
				}
			}
		}, delay, TimeUnit.MILLISECONDS);
	}

	protected void acquireLease(ConsumerLeaseKey key) {
		long nextTryTime = m_systemClockService.now();
		while (!isClosed() && !Thread.currentThread().isInterrupted()) {
			try {
				waitForNextTryTime(nextTryTime);

				if (isClosed()) {
					return;
				}

				LeaseAcquireResponse response = m_leaseManager.tryAcquireLease(key);

				if (response != null && response.isAcquired() && !response.getLease().isExpired()) {
					m_lease.set(response.getLease());
					scheduleRenewLeaseTask(key,
					      m_lease.get().getRemainingTime() - m_config.getRenewLeaseTimeMillisBeforeExpired());

					if (log.isDebugEnabled()) {
						log.debug(
						      "Acquire consumer lease success(topic={}, partition={}, groupId={}, sessionId={}, leaseId={}, expireTime={})",
						      m_context.getTopic().getName(), m_partitionId, m_context.getGroupId(),
						      m_context.getSessionId(), response.getLease().getId(), new Date(response.getLease()
						            .getExpireTime()));
					}
					return;
				} else {
					if (response != null && response.getNextTryTime() > 0) {
						nextTryTime = response.getNextTryTime();
					} else {
						nextTryTime = m_systemClockService.now() + m_config.getDefaultLeaseAcquireDelayMillis();
					}

					if (log.isDebugEnabled()) {
						log.debug(
						      "Unable to acquire consumer lease(topic={}, partition={}, groupId={}, sessionId={}), ignore it",
						      m_context.getTopic().getName(), m_partitionId, m_context.getGroupId(), m_context.getSessionId());
					}
				}
			} catch (Exception e) {
				log.error("Exception occurred while acquiring lease(topic={}, partition={}, groupId={}, sessionId={})",
				      m_context.getTopic().getName(), m_partitionId, m_context.getGroupId(), m_context.getSessionId(), e);
			}
		}
	}

	protected void waitForNextTryTime(long nextTryTime) {
		while (true) {
			if (!isClosed() && !Thread.currentThread().isInterrupted()) {
				if (nextTryTime > m_systemClockService.now()) {
					LockSupport.parkUntil(nextTryTime);
				} else {
					break;
				}
			} else {
				return;
			}
		}
	}

	protected void consumeMessages(long token) {
		List<ConsumerMessage<?>> msgs = new ArrayList<ConsumerMessage<?>>();

		m_msgs.drainTo(msgs);

		m_consumerNotifier.messageReceived(token, msgs);
	}

	@SuppressWarnings("rawtypes")
	protected List<ConsumerMessage<?>> decodeBatches(List<TppConsumerMessageBatch> batches, Class bodyClazz) {
		try {
			List<ConsumerMessage<?>> msgs = new ArrayList<ConsumerMessage<?>>();
			for (TppConsumerMessageBatch batch : batches) {
				List<MessageMeta> msgMetas = batch.getMessageMetas();
				ByteBuf batchData = batch.getData();

				int partition = batch.getPartition();

				for (int j = 0; j < msgMetas.size(); j++) {
					BaseConsumerMessage baseMsg = PlexusComponentLocator.lookup(MessageCodec.class).decode(batch.getTopic(),
					      batchData, bodyClazz);
					BrokerConsumerMessage brokerMsg = new BrokerConsumerMessage(baseMsg);
					MessageMeta messageMeta = msgMetas.get(j);
					brokerMsg.setPartition(partition);
					brokerMsg.setPriority(messageMeta.getPriority() == 0 ? true : false);
					brokerMsg.setResend(messageMeta.isResend());
					brokerMsg.setRetryTimesOfRetryPolicy(m_retryPolicy.getRetryTimes());
					brokerMsg.setMsgSeq(messageMeta.getId());

					msgs.add(decorateBrokerMessage(brokerMsg));
				}
			}

			return msgs;
		} catch (Exception e) {
			log.error("Failed to deserialize msg, because type mismatch between producer and consumer.", e);
			throw e;
		}
	}

	protected BrokerConsumerMessage<?> decorateBrokerMessage(BrokerConsumerMessage<?> brokerMsg) {
		return brokerMsg;
	}

	public void close() {
		m_closed.set(true);
		m_consumerNotifier.deregister(m_token.get(), true);
	}

	protected class BasePullMessagesTask implements Runnable {
		protected long m_token;

		protected AtomicReference<Offset> m_baseOffset;

		protected SchedulePolicy m_noEndpointSchedulePolicy;

		public BasePullMessagesTask(long token, AtomicReference<Offset> offset, SchedulePolicy noEndpointSchedulePolicy) {
			m_token = token;
			m_baseOffset = offset;
			m_noEndpointSchedulePolicy = noEndpointSchedulePolicy;
		}

		@Override
		public void run() {
			try {
				if (isClosed() || !m_msgs.isEmpty()) {
					return;
				}

				EndpointManager endpointManager = PlexusComponentLocator.lookup(EndpointManager.class);
				Endpoint endpoint = endpointManager.getEndpoint(m_context.getTopic().getName(), m_partitionId);

				if (endpoint == null) {
					log.warn("No endpoint found for topic {} partition {}, will retry later",
					      m_context.getTopic().getName(), m_partitionId);
					endpointManager.refreshEndpoint(m_context.getTopic().getName(), m_partitionId);
					m_noEndpointSchedulePolicy.fail(true);
					return;
				} else {
					m_noEndpointSchedulePolicy.succeess();
				}

				Lease lease = m_lease.get();
				if (lease != null) {
					long timeout = lease.getRemainingTime();

					if (timeout > 0) {
						pullMessages(endpoint, timeout);
					}
				}
			} catch (Exception e) {
				log.warn("Exception occurred while pulling message(topic={}, partition={}, groupId={}, sessionId={}).",
				      m_context.getTopic().getName(), m_partitionId, m_context.getGroupId(), m_context.getSessionId(), e);
			} finally {
				m_pullTaskRunning.set(false);
			}
		}

		protected void pullMessages(Endpoint endpoint, long timeout) throws InterruptedException, TimeoutException,
		      ExecutionException {
			final SettableFuture<PullMessageResultCommandV5> resultFuture = SettableFuture.create();

			PullMessageCommandV5 cmd = createPullMessageCommand(timeout);

			cmd.setFuture(resultFuture);

			PullMessageResultCommandV5 result = null;

			PullMessageResultMonitor pullMessageResultMonitor = PlexusComponentLocator
			      .lookup(PullMessageResultMonitor.class);
			PullMessageAcceptanceMonitor pullMessageAcceptMonitor = PlexusComponentLocator
			      .lookup(PullMessageAcceptanceMonitor.class);

			try {

				pullMessageResultMonitor.monitor(cmd);
				Future<Pair<Boolean, Endpoint>> acceptFuture = pullMessageAcceptMonitor.monitor(cmd.getHeader()
				      .getCorrelationId());

				long acceptTimeout = m_config.getPullMessageAcceptTimeoutMillis() > timeout ? timeout : m_config
				      .getPullMessageAcceptTimeoutMillis();

				if (PlexusComponentLocator.lookup(EndpointClient.class).writeCommand(endpoint, cmd)) {

					Pair<Boolean, Endpoint> acceptResult = waitForBrokerAcceptance(acceptFuture, acceptTimeout);

					if (acceptResult != null) {
						result = waitForBrokerResultIfNecessary(cmd, resultFuture, acceptResult, timeout);
					} else {
						PlexusComponentLocator.lookup(EndpointManager.class).refreshEndpoint(cmd.getTopic(),
						      cmd.getPartition());
					}

					if (result != null) {
						appendToMsgQueue(result);
						resultReceived(result);
					}
				} else {
					PlexusComponentLocator.lookup(EndpointManager.class).refreshEndpoint(cmd.getTopic(), cmd.getPartition());
				}
			} finally {
				pullMessageResultMonitor.remove(cmd);
				pullMessageAcceptMonitor.cancel(cmd.getHeader().getCorrelationId());
				if (result != null) {
					result.release();
				}
			}
		}

		private Pair<Boolean, Endpoint> waitForBrokerAcceptance(Future<Pair<Boolean, Endpoint>> acceptFuture, long timeout)
		      throws InterruptedException, ExecutionException {
			Pair<Boolean, Endpoint> acceptResult = null;
			try {
				acceptResult = acceptFuture.get(timeout, TimeUnit.MILLISECONDS);
			} catch (Exception e) {
				// ignore
			}
			return acceptResult;
		}

		private PullMessageResultCommandV5 waitForBrokerResultIfNecessary(PullMessageCommandV5 cmd,
		      SettableFuture<PullMessageResultCommandV5> resultFuture, Pair<Boolean, Endpoint> acceptResult, long timeout)
		      throws InterruptedException, ExecutionException {
			Boolean brokerAccept = acceptResult.getKey();
			String topic = cmd.getTopic();
			int partition = cmd.getPartition();
			if (brokerAccept != null && brokerAccept) {
				return waitForBrokerResult(resultFuture, timeout);
			} else {
				Endpoint newEndpoint = acceptResult.getValue();
				if (newEndpoint != null) {
					PlexusComponentLocator.lookup(EndpointManager.class).updateEndpoint(topic, partition, newEndpoint);
				}
				return null;
			}
		}

		private PullMessageResultCommandV5 waitForBrokerResult(Future<PullMessageResultCommandV5> resultFuture,
		      long timeout) throws InterruptedException, ExecutionException {
			PullMessageResultCommandV5 result = null;
			try {
				result = resultFuture.get(timeout, TimeUnit.MILLISECONDS);
			} catch (TimeoutException e) {
				// do nothing
			}

			return result;
		}

		protected void resultReceived(PullMessageResultCommandV5 ack) {
			if (ack.getOffset() != null) {
				m_baseOffset.set(ack.getOffset());
			}
		}

		protected PullMessageCommandV5 createPullMessageCommand(long timeout) {
			PullMessageCommandV5 cmd = new PullMessageCommandV5( //
			      m_context.getTopic().getName(), //
			      m_partitionId, //
			      m_context.getGroupId(), //
			      m_baseOffset.get(), //
			      m_msgs.remainingCapacity(), //
			      timeout, //
			      m_context.getMessageListenerConfig() instanceof FilterMessageListenerConfig ? //
			      ((FilterMessageListenerConfig) m_context.getMessageListenerConfig()).getFilter()
			            : null);

			cmd.getHeader().addProperty("pullTime", String.valueOf(m_systemClockService.now()));

			return cmd;
		}

		protected void appendToMsgQueue(PullMessageResultCommandV5 ack) {
			List<TppConsumerMessageBatch> batches = ack.getBatches();
			if (batches != null && !batches.isEmpty()) {
				ConsumerContext context = m_consumerNotifier.find(m_token);
				if (context != null) {
					Class<?> bodyClazz = context.getMessageClazz();

					List<ConsumerMessage<?>> msgs = decodeBatches(batches, bodyClazz);

					for (ConsumerMessage<?> msg : msgs) {
						m_ackManager.delivered(m_token, msg);
					}

					m_msgs.addAll(msgs);

				} else {
					log.info(
					      "Can not find consumerContext(topic={}, partition={}, groupId={}, sessionId={}), maybe has been stopped.",
					      m_context.getTopic().getName(), m_partitionId, m_context.getGroupId(), m_context.getSessionId());
				}
			}
		}

	}
}
