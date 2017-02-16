package com.ctrip.hermes.consumer.engine.ack;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.consumer.engine.ack.AckHolder.AckCallback;
import com.ctrip.hermes.consumer.engine.ack.AckHolder.NackCallback;
import com.ctrip.hermes.consumer.engine.config.ConsumerConfig;
import com.ctrip.hermes.consumer.engine.monitor.AckMessageAcceptanceMonitor;
import com.ctrip.hermes.consumer.engine.monitor.AckMessageResultMonitor;
import com.ctrip.hermes.consumer.engine.status.ConsumerStatusMonitor;
import com.ctrip.hermes.core.bo.AckContext;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.constants.CatConstants;
import com.ctrip.hermes.core.message.BaseConsumerMessage;
import com.ctrip.hermes.core.message.BaseConsumerMessageAware;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.transport.command.v5.AckMessageCommandV5;
import com.ctrip.hermes.core.transport.endpoint.EndpointClient;
import com.ctrip.hermes.core.transport.endpoint.EndpointManager;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = AckManager.class)
public class DefaultAckManager implements AckManager {

	private static final Logger log = LoggerFactory.getLogger(DefaultAckManager.class);

	@Inject
	private EndpointManager m_endpointManager;

	@Inject
	private EndpointClient m_endpointClient;

	@Inject
	private ConsumerConfig m_config;

	@Inject
	private AckMessageResultMonitor m_resultMonitor;

	@Inject
	private AckMessageAcceptanceMonitor m_acceptMonitor;

	private AtomicBoolean m_started = new AtomicBoolean(false);

	private ExecutorService m_ackCheckerIoExecutor;

	private ConcurrentMap<Long, TpgAckHolder> m_ackHolders = new ConcurrentHashMap<>();

	@Override
	public void register(long token, Tpg tpg, int maxAckHolderSize) {
		if (m_started.compareAndSet(false, true)) {
			startChecker();
		}

		if (!m_ackHolders.containsKey(token)) {
			m_ackHolders.putIfAbsent(token, new TpgAckHolder(tpg, token, maxAckHolderSize));
		}
	}

	@Override
	public void ack(long token, ConsumerMessage<?> msg) {
		TpgAckHolder holder = findTpgAckHolder(msg.getTopic(), msg.getPartition(), token);
		if (holder != null) {
			BaseConsumerMessage<?> bcm = msg instanceof BaseConsumerMessageAware ? ((BaseConsumerMessageAware<?>) msg)
			      .getBaseConsumerMessage() : null;

			long onMessageStart = bcm == null ? -1L : bcm.getOnMessageStartTimeMills();
			long onMessageEnd = bcm == null ? -1L : bcm.getOnMessageEndTimeMills();
			holder.offerOperation(new AckOperation(msg.getOffset(), msg.isPriority(), msg.isResend(), onMessageStart,
			      onMessageEnd));
		}
	}

	@Override
	public void nack(long token, ConsumerMessage<?> msg) {
		TpgAckHolder holder = findTpgAckHolder(msg.getTopic(), msg.getPartition(), token);
		if (holder != null) {
			BaseConsumerMessage<?> bcm = msg instanceof BaseConsumerMessageAware ? ((BaseConsumerMessageAware<?>) msg)
			      .getBaseConsumerMessage() : null;

			long onMessageStart = bcm == null ? -1L : bcm.getOnMessageStartTimeMills();
			long onMessageEnd = bcm == null ? -1L : bcm.getOnMessageEndTimeMills();
			holder.offerOperation(new NackOperation(msg.getOffset(), msg.isPriority(), msg.isResend(), onMessageStart,
			      onMessageEnd));
		}
	}

	@Override
	public void delivered(long token, ConsumerMessage<?> msg) {
		TpgAckHolder holder = findTpgAckHolder(msg.getTopic(), msg.getPartition(), token);
		if (holder != null) {
			holder.offerOperation(new DeliveredOperation(msg.getOffset(), msg.isPriority(), msg.isResend(), msg
			      .getRemainingRetries()));
		}
	}

	private TpgAckHolder findTpgAckHolder(String topic, int partition, long token) {
		TpgAckHolder holder = m_ackHolders.get(token);
		if (holder == null) {
			log.warn("TpgAckHolder not found(topic={}, partition={}).", topic, partition);
			return null;
		} else {
			return holder;
		}
	}

	@Override
	public void deregister(long token) {
		TpgAckHolder holder = m_ackHolders.get(token);

		if (holder != null) {
			holder.stop();

			long expireTime = System.currentTimeMillis() + 5000;

			while (!Thread.interrupted() && System.currentTimeMillis() < expireTime && holder.hasUnhandleOperation()
			      && m_ackHolders.containsKey(token)) {
				try {
					TimeUnit.MILLISECONDS.sleep(50);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
		}
	}

	private void startChecker() {
		m_ackCheckerIoExecutor = Executors.newFixedThreadPool(m_config.getAckCheckerIoThreadCount(),
		      HermesThreadFactory.create("AckCheckerIo", true));

		Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create("AckChecker", true))
		      .scheduleWithFixedDelay(new AckHolderChecker(), m_config.getAckCheckerIntervalMillis(),
		            m_config.getAckCheckerIntervalMillis(), TimeUnit.MILLISECONDS);
	}

	@Override
	public boolean writeAckToBroker(AckMessageCommandV5 cmd) {
		String topic = cmd.getTopic();
		int partition = cmd.getPartition();
		String groupId = cmd.getGroup();

		Endpoint endpoint = m_endpointManager.getEndpoint(topic, partition);

		boolean acked = false;
		long correlationId = cmd.getHeader().getCorrelationId();
		if (endpoint != null) {
			long resultTimeout = cmd.getTimeout();
			int acceptTimeout = m_config.getAckCheckerAcceptTimeoutMillis();

			Transaction tx = Cat.newTransaction(CatConstants.TYPE_MESSAGE_CONSUME_ACK_TRANSPORT,
			      String.format("%s:%s", topic, groupId));
			try {
				Future<Pair<Boolean, Endpoint>> acceptFuture = m_acceptMonitor.monitor(correlationId);
				Future<Boolean> resultFuture = m_resultMonitor.monitor(correlationId);
				if (m_endpointClient.writeCommand(endpoint, cmd)) {

					Pair<Boolean, Endpoint> acceptResult = waitForBrokerAcceptance(cmd, acceptFuture, acceptTimeout);

					if (acceptResult != null) {
						acked = waitForBrokerResultIfNecessary(cmd, resultFuture, acceptResult, resultTimeout);
					} else {
						m_endpointManager.refreshEndpoint(topic, partition);
					}
				} else {
					m_endpointManager.refreshEndpoint(topic, partition);
				}

				tx.setStatus(acked ? Transaction.SUCCESS : "ACK_CMD_FAILED");
			} catch (Exception e) {
				tx.setStatus(e);
			} finally {
				m_acceptMonitor.cancel(correlationId);
				m_resultMonitor.cancel(correlationId);
				tx.complete();
			}

		} else {
			log.debug("No endpoint found, ignore it");
			m_endpointManager.refreshEndpoint(topic, partition);
		}
		return acked;
	}

	private Pair<Boolean, Endpoint> waitForBrokerAcceptance(AckMessageCommandV5 cmd,
	      Future<Pair<Boolean, Endpoint>> acceptFuture, long timeout) throws InterruptedException, ExecutionException {
		Pair<Boolean, Endpoint> acceptResult = null;
		try {
			acceptResult = acceptFuture.get(timeout, TimeUnit.MILLISECONDS);
		} catch (TimeoutException e) {
			// ignore
		}
		return acceptResult;
	}

	private boolean waitForBrokerResultIfNecessary(AckMessageCommandV5 cmd, Future<Boolean> resultFuture,
	      Pair<Boolean, Endpoint> acceptResult, long resultTimeout) throws InterruptedException, ExecutionException {
		Boolean brokerAccept = acceptResult.getKey();
		String topic = cmd.getTopic();
		int partition = cmd.getPartition();
		if (brokerAccept != null && brokerAccept) {
			return waitForBrokerResult(topic, partition, cmd.getGroup(), cmd.getHeader().getCorrelationId(), resultFuture,
			      resultTimeout);
		} else {
			Endpoint newEndpoint = acceptResult.getValue();
			if (newEndpoint != null) {
				m_endpointManager.updateEndpoint(topic, partition, newEndpoint);
			}
			return false;
		}
	}

	private boolean waitForBrokerResult(String topic, int partition, String groupId, long correlationId,
	      Future<Boolean> resultFuture, long timeout) {
		try {
			Boolean acked = resultFuture.get(timeout, TimeUnit.MILLISECONDS);

			if (acked != null && acked) {
				return true;
			} else {
				return false;
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		} catch (TimeoutException e) {
			// do nothing
		} catch (Exception e) {
			// do nothing
		}

		return false;
	}

	private class AckHolderChecker implements Runnable {

		public void run() {
			List<TpgAckHolder> holders = new ArrayList<>(m_ackHolders.values());
			Collections.shuffle(holders);

			for (TpgAckHolder holder : holders) {
				try {
					holder.handleOperations();
					check(holder);
				} catch (Exception e) {
					log.error("Exception occurred while executing AckChecker task.", e);
				}
			}
		}

		private void check(final TpgAckHolder holder) {
			if (holder.startFlush()) {
				m_ackCheckerIoExecutor.submit(new HolderFlushTask(holder));
			}
		}

		private class HolderFlushTask implements Runnable {

			private TpgAckHolder m_holder;

			public HolderFlushTask(TpgAckHolder holder) {
				m_holder = holder;
			}

			@Override
			public void run() {
				AckMessageCommandV5 cmd = m_holder.pop();
				try {
					if (cmd != null) {
						boolean success = writeAckToBroker(cmd);
						if (success) {
							cmd = null;
						}
					} else {
						if (m_holder.isStopped()) {
							m_ackHolders.remove(m_holder.getToken());
						}
					}
				} catch (Exception e) {
					log.warn("Exception occurred while flushing ack cmd to broker, will retry it", e);
				} finally {
					if (cmd != null) {
						m_holder.push(cmd);
					}

					m_holder.finishFlush();
				}
			}
		}
	}

	private class TpgAckHolder {
		private Tpg m_tpg;

		private int m_maxAckHolderSize;

		private long m_token;

		private AckHolder<AckContext> m_priorityAckHolder;

		private AckHolder<AckContext> m_nonpriorityAckHolder;

		private AckHolder<AckContext> m_resendAckHolder;

		private BlockingQueue<Operation> m_opQueue;

		private AtomicBoolean m_stopped = new AtomicBoolean(false);

		private AtomicReference<AckMessageCommandV5> m_cmd = new AtomicReference<>(null);

		private AtomicBoolean m_flushing = new AtomicBoolean(false);

		public TpgAckHolder(Tpg tpg, long token, int maxAckHolderSize) {
			m_tpg = tpg;
			m_token = token;
			m_maxAckHolderSize = maxAckHolderSize;

			m_priorityAckHolder = new DefaultAckHolder<>(m_tpg, m_maxAckHolderSize);
			m_nonpriorityAckHolder = new DefaultAckHolder<>(m_tpg, m_maxAckHolderSize);
			m_resendAckHolder = new DefaultAckHolder<>(m_tpg, m_maxAckHolderSize);
			m_opQueue = new LinkedBlockingQueue<>();
		}

		public void finishFlush() {
			m_flushing.set(false);
		}

		public boolean startFlush() {
			return m_flushing.compareAndSet(false, true);
		}

		public void stop() {
			m_stopped.set(true);
		}

		public AckMessageCommandV5 pop() {
			if (m_cmd.get() == null) {
				return scan();
			}
			return m_cmd.getAndSet(null);
		}

		public void push(AckMessageCommandV5 cmd) {
			m_cmd.set(cmd);
		}

		private AckMessageCommandV5 scan() {
			AckMessageCommandV5 cmd = null;

			AckHolderScanningResult<AckContext> priorityScanningRes = m_priorityAckHolder.scan(m_config
			      .getAckCommandMaxSize());
			AckHolderScanningResult<AckContext> nonpriorityScanningRes = m_nonpriorityAckHolder.scan(m_config
			      .getAckCommandMaxSize());
			AckHolderScanningResult<AckContext> resendScanningRes = m_resendAckHolder
			      .scan(m_config.getAckCommandMaxSize());

			if (!priorityScanningRes.getAcked().isEmpty() //
			      || !priorityScanningRes.getNacked().isEmpty() //
			      || !nonpriorityScanningRes.getAcked().isEmpty() //
			      || !nonpriorityScanningRes.getNacked().isEmpty() //
			      || !resendScanningRes.getAcked().isEmpty() //
			      || !resendScanningRes.getNacked().isEmpty()) {

				cmd = new AckMessageCommandV5(m_tpg.getTopic(), m_tpg.getPartition(), m_tpg.getGroupId(),
				      m_config.getAckCheckerResultTimeoutMillis());

				// priority
				for (AckContext ctx : priorityScanningRes.getAcked()) {
					cmd.addAckMsg(true, false, ctx.getMsgSeq(), ctx.getRemainingRetries(),
					      ctx.getOnMessageStartTimeMillis(), ctx.getOnMessageEndTimeMillis());
				}
				for (AckContext ctx : priorityScanningRes.getNacked()) {
					cmd.addNackMsg(true, false, ctx.getMsgSeq(), ctx.getRemainingRetries(),
					      ctx.getOnMessageStartTimeMillis(), ctx.getOnMessageEndTimeMillis());
				}
				// non-priority
				for (AckContext ctx : nonpriorityScanningRes.getAcked()) {
					cmd.addAckMsg(false, false, ctx.getMsgSeq(), ctx.getRemainingRetries(),
					      ctx.getOnMessageStartTimeMillis(), ctx.getOnMessageEndTimeMillis());
				}
				for (AckContext ctx : nonpriorityScanningRes.getNacked()) {
					cmd.addNackMsg(false, false, ctx.getMsgSeq(), ctx.getRemainingRetries(),
					      ctx.getOnMessageStartTimeMillis(), ctx.getOnMessageEndTimeMillis());
				}
				// resend
				for (AckContext ctx : resendScanningRes.getAcked()) {
					cmd.addAckMsg(false, true, ctx.getMsgSeq(), ctx.getRemainingRetries(),
					      ctx.getOnMessageStartTimeMillis(), ctx.getOnMessageEndTimeMillis());
				}
				for (AckContext ctx : resendScanningRes.getNacked()) {
					cmd.addNackMsg(false, true, ctx.getMsgSeq(), ctx.getRemainingRetries(),
					      ctx.getOnMessageStartTimeMillis(), ctx.getOnMessageEndTimeMillis());
				}

			}

			return cmd;
		}

		public long getToken() {
			return m_token;
		}

		private AckHolder<AckContext> findHolder(Operation op) {
			if (op.isResend()) {
				return m_resendAckHolder;
			} else {
				return op.isPriority() ? m_priorityAckHolder : m_nonpriorityAckHolder;
			}
		}

		public void handleOperations() {
			List<Operation> todos = new ArrayList<Operation>();

			m_opQueue.drainTo(todos);

			if (!todos.isEmpty()) {
				for (Operation op : todos) {
					try {
						AckHolder<AckContext> holder = findHolder(op);
						if (op instanceof DeliveredOperation) {
							DeliveredOperation dOp = (DeliveredOperation) op;

							holder.delivered(dOp.getId(), new AckContext(dOp.getId(), dOp.getRemainingRetries(), -1, -1));
							ConsumerStatusMonitor.INSTANCE.msgReceived(m_tpg);
						} else if (op instanceof AckOperation) {
							final AckOperation aOp = (AckOperation) op;
							holder.ack(aOp.getId(), new AckCallback<AckContext>() {

								@Override
								public void doBeforeAck(AckContext item) {
									item.setOnMessageStartTimeMillis(aOp.getOnMessageStart());
									item.setOnMessageEndTimeMillis(aOp.getOnMessageEnd());
								}
							});
							ConsumerStatusMonitor.INSTANCE.msgAcked(m_tpg, aOp.getOnMessageStart(), aOp.getOnMessageEnd(),
							      true);
						} else if (op instanceof NackOperation) {
							final NackOperation aOp = (NackOperation) op;
							holder.nack(aOp.getId(), new NackCallback<AckContext>() {

								@Override
								public void doBeforeNack(AckContext item) {
									item.setOnMessageStartTimeMillis(aOp.getOnMessageStart());
									item.setOnMessageEndTimeMillis(aOp.getOnMessageEnd());
								}
							});
							ConsumerStatusMonitor.INSTANCE.msgAcked(m_tpg, aOp.getOnMessageStart(), aOp.getOnMessageEnd(),
							      false);
						}
					} catch (AckHolderException e) {
						log.error("Exception occurred while handling operations({}).", m_tpg, e);
						Cat.logError(e);
					}
				}
			}
		}

		public boolean isStopped() {
			return m_stopped.get();
		}

		public void offerOperation(Operation op) {
			if (!m_stopped.get()) {
				m_opQueue.offer(op);
			}
		}

		public boolean hasUnhandleOperation() {
			return !m_opQueue.isEmpty();
		}
	}

	static class Operation {
		private long m_id;

		private boolean m_priority;

		private boolean m_resend;

		public Operation(long id, boolean priority, boolean resend) {
			m_id = id;
			m_priority = priority;
			m_resend = resend;
		}

		public long getId() {
			return m_id;
		}

		public boolean isPriority() {
			return m_priority;
		}

		public boolean isResend() {
			return m_resend;
		}

	}

	static class DeliveredOperation extends Operation {

		private int m_remainingRetries;

		public DeliveredOperation(long id, boolean priority, boolean resend, int remainingRetries) {
			super(id, priority, resend);
			m_remainingRetries = remainingRetries;
		}

		public int getRemainingRetries() {
			return m_remainingRetries;
		}

	}

	static class NackOperation extends Operation {

		private long m_onMessageStart;

		private long m_onMessageEnd;

		public NackOperation(long id, boolean priority, boolean resend, long onMessageStart, long onMessageEnd) {
			super(id, priority, resend);
			m_onMessageStart = onMessageStart;
			m_onMessageEnd = onMessageEnd;
		}

		public long getOnMessageStart() {
			return m_onMessageStart;
		}

		public long getOnMessageEnd() {
			return m_onMessageEnd;
		}

	}

	static class AckOperation extends Operation {

		private long m_onMessageStart;

		private long m_onMessageEnd;

		public AckOperation(long id, boolean priority, boolean resend, long onMessageStart, long onMessageEnd) {
			super(id, priority, resend);
			m_onMessageStart = onMessageStart;
			m_onMessageEnd = onMessageEnd;
		}

		public long getOnMessageStart() {
			return m_onMessageStart;
		}

		public long getOnMessageEnd() {
			return m_onMessageEnd;
		}

	}

}
