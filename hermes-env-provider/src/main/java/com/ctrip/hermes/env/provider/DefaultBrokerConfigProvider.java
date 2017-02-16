package com.ctrip.hermes.env.provider;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.env.ClientEnvironment;
import com.ctrip.hermes.env.config.broker.BrokerConfigProvider;
import com.ctrip.hermes.env.config.broker.MySQLCacheConfigProvider;
import com.google.common.util.concurrent.RateLimiter;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = BrokerConfigProvider.class)
public class DefaultBrokerConfigProvider implements BrokerConfigProvider,
		Initializable {

	private static final Logger log = LoggerFactory
			.getLogger(DefaultBrokerConfigProvider.class);

	@Inject
	private ClientEnvironment m_env;

	private String m_sessionId;

	private static final int DEFAULT_BROKER_PORT = 4376;

	private static final int DEFAULT_SHUTDOWN_PORT = 4888;

	private DefaultMySQLCacheConfigProvider m_cacheConfig = new DefaultMySQLCacheConfigProvider();

	private int m_filterPatternCacheSize = 10000;

	private int m_filterPerTopicCacheSize = 20;

	private int m_filterTopicCacheSize = 5000;

	private AtomicReference<Map<String, Double>> m_topicQPSRateLimits = new AtomicReference<Map<String, Double>>(
			new HashMap<String, Double>());

	private Map<Pair<String, Integer>, RateLimiter> m_topicPartitionQPSRateLimiters = new ConcurrentHashMap<>();

	private AtomicReference<Map<String, Double>> m_topicBytesRateLimits = new AtomicReference<Map<String, Double>>(
			new HashMap<String, Double>());

	private Map<Pair<String, Integer>, RateLimiter> m_topicPartitionBytesRateLimiters = new ConcurrentHashMap<>();

	public DefaultBrokerConfigProvider() {
		m_sessionId = System.getProperty("brokerId", UUID.randomUUID()
				.toString());
	}

	@Override
	public void initialize() throws InitializationException {

		m_cacheConfig.init(m_env.getGlobalConfig());

	}

	public String getSessionId() {
		return m_sessionId;
	}

	public String getRegistryName(String name) {
		return "default";
	}

	public String getRegistryBasePath() {
		return "brokers";
	}

	public long getLeaseRenewTimeMillsBeforeExpire() {
		return 2000L;
	}

	public int getLongPollingServiceThreadCount() {
		return 50;
	}

	public int getMessageQueueFlushCountLimit(String topic) {
		return 1000;
	}

	public boolean isMessageQueueFlushLimitDynamicAdjust(String topic) {
		return false;
	}

	public long getAckOpCheckIntervalMillis() {
		return 200;
	}

	public int getAckOpHandlingBatchSize() {
		return 5000;
	}

	public int getAckOpExecutorThreadCount() {
		return 10;
	}

	public int getAckOpQueueSize() {
		return 500000;
	}

	public int getLeaseContainerThreadCount() {
		return 10;
	}

	public long getDefaultLeaseRenewDelayMillis() {
		return 500L;
	}

	public long getDefaultLeaseAcquireDelayMillis() {
		return 100L;
	}

	public int getListeningPort() {
		String port = System.getProperty("brokerPort");
		if (!StringUtils.isNumeric(port)) {
			return DEFAULT_BROKER_PORT;
		} else {
			return Integer.valueOf(port);
		}
	}

	public int getClientMaxIdleSeconds() {
		return 3600;
	}

	public int getShutdownRequestPort() {
		String port = System.getProperty("brokerShutdownPort");
		if (!StringUtils.isNumeric(port)) {
			return DEFAULT_SHUTDOWN_PORT;
		} else {
			return Integer.valueOf(port);
		}
	}

	public int getMessageOffsetQueryPrecisionMillis() {
		return 30000;
	}

	public int getFetchMessageWithOffsetBatchSize() {
		return 500;
	}

	public int getAckMessagesTaskQueueSize() {
		return 500000;
	}

	public int getAckMessagesTaskExecutorThreadCount() {
		return 10;
	}

	public long getAckMessagesTaskExecutorCheckIntervalMillis() {
		return 100;
	}

	public MySQLCacheConfigProvider getMySQLCacheConfig() {
		return m_cacheConfig;
	}

	public int getFilterPatternCacheSize() {
		return m_filterPatternCacheSize;
	}

	public int getFilterPerTopicCacheSize() {
		return m_filterPerTopicCacheSize;
	}

	public int getFilterTopicCacheSize() {
		return m_filterTopicCacheSize;
	}

	public int getPullMessageSelectorWriteOffsetTtlMillis() {
		return 8000;
	}

	public int getPullMessageSelectorSafeTriggerIntervalMillis() {
		return 100;
	}

	public int getPullMessageSelectorOffsetLoaderThreadPoolSize() {
		return 50;
	}

	public int getPullMessageSelectorOffsetLoaderThreadPoolKeepaliveSeconds() {
		return 60;
	}

	public long getPullMessageSelectorSafeTriggerMinFireIntervalMillis() {
		return 100;
	}

	public int getSendMessageSelectorSafeTriggerMinFireIntervalMillis() {
		return 10;
	}

	public int getSendMessageSelectorSafeTriggerIntervalMillis() {
		return 10;
	}

	public int getSendMessageSelectorNormalTriggeringOffsetDelta(String topic) {
		return 20;
	}

	public long getSendMessageSelectorSafeTriggerTriggeringOffsetDelta(
			String topic) {
		return 20L;
	}

	public int getPullMessageSelectorNormalTriggeringOffsetDelta(String topic,
			String groupId) {
		return 1;
	}

	public long getPullMessageSelectorSafeTriggerTriggeringOffsetDelta(
			String topic, String groupId) {
		return 100L;
	}

	public int getMessageQueueFlushThreadCount() {
		return 500;
	}

	public long getMessageQueueFetchPriorityMessageBySafeTriggerMinInterval() {
		return 200L;
	}

	public long getMessageQueueFetchNonPriorityMessageBySafeTriggerMinInterval() {
		return 200L;
	}

	public long getMessageQueueFetchResendMessageBySafeTriggerMinInterval() {
		return 200L;
	}

	public RateLimiter getPartitionProduceQPSRateLimiter(String topic,
			int partition) {
		Double limit = getLimit(m_topicQPSRateLimits.get(), topic);

		Pair<String, Integer> tp = new Pair<String, Integer>(topic, partition);
		RateLimiter rateLimiter = m_topicPartitionQPSRateLimiters.get(tp);

		if (rateLimiter == null) {
			synchronized (m_topicPartitionQPSRateLimiters) {
				rateLimiter = m_topicPartitionQPSRateLimiters.get(tp);
				if (rateLimiter == null) {
					rateLimiter = RateLimiter.create(limit);
					m_topicPartitionQPSRateLimiters.put(tp, rateLimiter);
					log.info(
							"Set single partition's qps rate limit to {} for topic {} and partition {}",
							limit, topic, partition);
				}
			}
		} else {
			synchronized (rateLimiter) {
				if (rateLimiter.getRate() != limit) {
					rateLimiter.setRate(limit);
					log.info(
							"Single partition's qps rate limit changed to {} for topic {} and partition {}",
							limit, topic, partition);
				}
			}
		}
		return rateLimiter;
	}

	public RateLimiter getPartitionProduceBytesRateLimiter(String topic,
			int partition) {
		Double limit = getLimit(m_topicBytesRateLimits.get(), topic);

		Pair<String, Integer> tp = new Pair<String, Integer>(topic, partition);
		RateLimiter rateLimiter = m_topicPartitionBytesRateLimiters.get(tp);

		if (rateLimiter == null) {
			synchronized (m_topicPartitionBytesRateLimiters) {
				rateLimiter = m_topicPartitionBytesRateLimiters.get(tp);
				if (rateLimiter == null) {
					rateLimiter = RateLimiter.create(limit);
					m_topicPartitionBytesRateLimiters.put(tp, rateLimiter);
					log.info(
							"Set single partition's bytes rate limit to {} for topic {}",
							limit, topic);
				}
			}
		} else {
			synchronized (rateLimiter) {
				if (rateLimiter.getRate() != limit) {
					rateLimiter.setRate(limit);
					log.info(
							"Single partition's bytes rate limit changed to {} for topic {}",
							limit, topic);
				}
			}
		}
		return rateLimiter;
	}

	private Double getLimit(Map<String, Double> limits, String topic) {
		return Double.MAX_VALUE;
	}

	@Override
	public long getAckFlushSelectorSafeTriggerIntervalMillis() {
		return 100;
	}

	@Override
	public int getAckFlushSelectorNormalTriggeringOffsetDeltas(String topic) {
		return 100;
	}

	@Override
	public long getAckFlushSelectorSafeTriggerTriggeringOffsetDeltas(
			String topic) {
		return 1000L;
	}

	@Override
	public boolean isBizLoggerEnabled() {
		return true;
	}

	@Override
	public int getAckFlushThreadCount() {
		return 30;
	}

	@Override
	public int getMessageQueueFlushBatchSize() {
		return 1000;
	}
}
