package com.ctrip.hermes.env.config.broker;

import com.google.common.util.concurrent.RateLimiter;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface BrokerConfigProvider {

	public String getSessionId();

	public String getRegistryName(String name);

	public String getRegistryBasePath();

	public long getLeaseRenewTimeMillsBeforeExpire();

	public int getLongPollingServiceThreadCount();

	public int getMessageQueueFlushBatchSize();

	public long getAckOpCheckIntervalMillis();

	public int getAckOpHandlingBatchSize();

	public int getAckOpExecutorThreadCount();

	public int getAckOpQueueSize();

	public int getLeaseContainerThreadCount();

	public long getDefaultLeaseRenewDelayMillis();

	public long getDefaultLeaseAcquireDelayMillis();

	public int getListeningPort();

	public int getClientMaxIdleSeconds();

	public int getShutdownRequestPort();

	public int getMessageOffsetQueryPrecisionMillis();

	public int getFetchMessageWithOffsetBatchSize();

	public int getAckMessagesTaskQueueSize();

	public int getAckMessagesTaskExecutorThreadCount();

	public long getAckMessagesTaskExecutorCheckIntervalMillis();

	public MySQLCacheConfigProvider getMySQLCacheConfig();

	public int getFilterPatternCacheSize();

	public int getFilterPerTopicCacheSize();

	public int getFilterTopicCacheSize();

	public int getPullMessageSelectorWriteOffsetTtlMillis();

	public int getPullMessageSelectorSafeTriggerIntervalMillis();

	public int getPullMessageSelectorOffsetLoaderThreadPoolSize();

	public int getPullMessageSelectorOffsetLoaderThreadPoolKeepaliveSeconds();

	public long getPullMessageSelectorSafeTriggerMinFireIntervalMillis();

	public int getSendMessageSelectorSafeTriggerMinFireIntervalMillis();

	public int getSendMessageSelectorSafeTriggerIntervalMillis();

	public int getSendMessageSelectorNormalTriggeringOffsetDelta(String topic);

	public long getSendMessageSelectorSafeTriggerTriggeringOffsetDelta(String topic);

	public int getPullMessageSelectorNormalTriggeringOffsetDelta(String topic, String groupId);

	public long getPullMessageSelectorSafeTriggerTriggeringOffsetDelta(String topic, String groupId);

	public int getMessageQueueFlushThreadCount();

	public long getMessageQueueFetchPriorityMessageBySafeTriggerMinInterval();

	public long getMessageQueueFetchNonPriorityMessageBySafeTriggerMinInterval();

	public long getMessageQueueFetchResendMessageBySafeTriggerMinInterval();

	public RateLimiter getPartitionProduceBytesRateLimiter(String topic, int partition);

	public RateLimiter getPartitionProduceQPSRateLimiter(String topic, int partition);

	public int getMessageQueueFlushCountLimit(String topic);

	public boolean isMessageQueueFlushLimitDynamicAdjust(String topic);

	public long getAckFlushSelectorSafeTriggerIntervalMillis();

	public int getAckFlushSelectorNormalTriggeringOffsetDeltas(String topic);

	public long getAckFlushSelectorSafeTriggerTriggeringOffsetDeltas(String topic);
	
	public boolean isBizLoggerEnabled();

	public int getAckFlushThreadCount();
}
