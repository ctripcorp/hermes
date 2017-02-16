package com.ctrip.hermes.consumer.engine.notifier;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Triple;

import com.ctrip.hermes.consumer.ConsumerType;
import com.ctrip.hermes.consumer.api.MessageListenerConfig;
import com.ctrip.hermes.consumer.build.BuildConstants;
import com.ctrip.hermes.consumer.engine.ConsumerContext;
import com.ctrip.hermes.consumer.engine.config.ConsumerConfig;
import com.ctrip.hermes.consumer.engine.status.ConsumerStatusMonitor;
import com.ctrip.hermes.consumer.message.BrokerConsumerMessage;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.message.DummyBaseConsumerMessage;
import com.ctrip.hermes.core.pipeline.Pipeline;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.meta.entity.Storage;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = ConsumerNotifier.class)
public class DefaultConsumerNotifier implements ConsumerNotifier {

	private static final Logger log = LoggerFactory.getLogger(DefaultConsumerNotifier.class);

	private ConcurrentMap<Long, Triple<ConsumerContext, NotifyStrategy, ExecutorService>> m_consumerContexs = new ConcurrentHashMap<Long, Triple<ConsumerContext, NotifyStrategy, ExecutorService>>();

	@Inject(BuildConstants.CONSUMER)
	private Pipeline<Void> m_pipeline;

	@Inject
	private ConsumerConfig m_config;

	@Inject
	private SystemClockService m_systemClockService;

	@Override
	public void register(long token, final ConsumerContext context) {
		try {
			if (log.isDebugEnabled()) {
				log.debug("Registered(token={}, topic={}, groupId={}, sessionId={})", token, context.getTopic().getName(),
				      context.getGroupId(), context.getSessionId());
			}

			int threadCount = m_config.getNotifierThreadCount(context.getTopic().getName());
			int notifierWorkQueueSize = m_config.getNotifierWorkQueueSize(context.getTopic().getName());

			MessageListenerConfig messageListenerConfig = context.getMessageListenerConfig();
			NotifyStrategy notifyStrategy = null;
			String storageType = context.getTopic().getStorageType();

			if (Storage.KAFKA.equals(storageType)) {
				notifyStrategy = new DefaultNotifyStrategy();
			} else {
				if (context.getConsumerType() == ConsumerType.PULL) {
					notifyStrategy = new PullNotifyStrategy();
				} else if (messageListenerConfig.isStrictlyOrdering()) {
					notifyStrategy = new StrictlyOrderedNotifyStrategy(
					      messageListenerConfig.getStrictlyOrderingRetryPolicy());
				} else {
					notifyStrategy = new DefaultNotifyStrategy();
				}
			}

			if (!m_consumerContexs.containsKey(token)) {
				m_consumerContexs.putIfAbsent(token, new Triple<ConsumerContext, NotifyStrategy, ExecutorService>(context,
				      notifyStrategy, createNotifierExecutor(context, threadCount, notifierWorkQueueSize, token)));
			}
		} catch (Exception e) {
			throw new RuntimeException("Register consumer notifier failed", e);
		}
	}

	@Override
	public void deregister(long token, boolean force) {
			Triple<ConsumerContext, NotifyStrategy, ExecutorService> triple = m_consumerContexs.remove(token);
			if (triple != null) {
				ConsumerContext context = triple.getFirst();
				if (log.isDebugEnabled()) {
					log.debug("Deregistered(token={}, topic={}, groupId={}, sessionId={})", token, context.getTopic()
					      .getName(), context.getGroupId(), context.getSessionId());
				}
				if (!force) {
					triple.getLast().shutdown();
				} else {
					triple.getLast().shutdownNow();
				}
			}
	}

	@Override
	public void messageReceived(final long token, final List<ConsumerMessage<?>> msgs) {
		Triple<ConsumerContext, NotifyStrategy, ExecutorService> triple = m_consumerContexs.get(token);
		final ConsumerContext context = triple.getFirst();
		final NotifyStrategy notifyStrategy = triple.getMiddle();
		final ExecutorService executorService = triple.getLast();

		executorService.submit(new Runnable() {

			@SuppressWarnings({ "rawtypes" })
			@Override
			public void run() {
				if (msgs == null || msgs.isEmpty()) {
					return;
				}
				try {
					Iterator<ConsumerMessage<?>> iter = msgs.iterator();
					while (iter.hasNext()) {
						ConsumerMessage<?> msg = iter.next();
						if (msg instanceof BrokerConsumerMessage) {
							BrokerConsumerMessage bmsg = (BrokerConsumerMessage) msg;
							bmsg.setToken(token);
							bmsg.setGroupId(context.getGroupId());

							if (bmsg.getBaseConsumerMessage() instanceof DummyBaseConsumerMessage) {
								msg.ack();
								iter.remove();
							}
						}
					}

					long start = System.currentTimeMillis();
					notifyStrategy.notify(msgs, context, executorService, m_pipeline);

					long avgDuration = (System.currentTimeMillis() - start) / msgs.size();

					ConsumerStatusMonitor.INSTANCE.msgProcessed(context.getTopic().getName(), msgs.get(0).getPartition(),
					      context.getGroupId(), msgs.size(), avgDuration);

				} catch (Exception e) {
					log.error(
					      "Exception occurred while calling messageReceived(token={}, topic={}, groupId={}, sessionId={})",
					      token, context.getTopic().getName(), context.getGroupId(), context.getSessionId(), e);
				}
			}
		});

	}

	@Override
	public ConsumerContext find(long token) {
		Triple<ConsumerContext, NotifyStrategy, ExecutorService> triple = m_consumerContexs.get(token);
		return triple == null ? null : triple.getFirst();
	}

	private ExecutorService createNotifierExecutor(ConsumerContext context, int threadCount, int notifierWorkQueueSize,
	      long token) {
		return new ThreadPoolExecutor(threadCount, //
		      threadCount,//
		      0L, //
		      TimeUnit.MILLISECONDS,//

		      new LinkedBlockingQueue<Runnable>(notifierWorkQueueSize), //
		      HermesThreadFactory.create(
		            String.format("ConsumerNotifier-%s-%s-%s", context.getTopic().getName(), context.getGroupId(), token),
		            false), //
		      new BlockUntilSubmittedPolicy());
	}

	private static class BlockUntilSubmittedPolicy implements RejectedExecutionHandler {

		@Override
		public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
			try {
				executor.getQueue().put(r);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}

	}
}
