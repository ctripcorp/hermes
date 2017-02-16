package com.ctrip.hermes.producer.config;

import java.util.HashMap;
import java.util.Map;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.env.ClientEnvironment;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = ProducerConfig.class)
public class ProducerConfig implements Initializable {

	public static final long DEFAULT_BROKER_SENDER_ACCEPT_TIMEOUT = 4 * 1000L;

	public static final long DEFAULT_BROKER_SENDER_RESULT_TIMEOUT = 4 * 1000L;

	public static final int DEFAULT_BROKER_SENDER_TASK_QUEUE_SIZE = 200000;

	public static final int DEFAULT_BROKER_SENDER_BATCH_SIZE = 500;

	public static final int DEFAULT_PRODUCER_CALLBACK_THREAD_COUNT = 10;

	private static final int DEFAULT_BROKER_SENDER_CONCURRENT_LEVEL = 2;

	private static final int DEFAULT_MESSAGE_SENDER_SELECTOR_NORMAL_TRIGGERING_OFFSET_DELTA = 1;

	private static final int DEFAULT_MESSAGE_SENDER_SELECTOR_SAFE_TRIGGER_TRIGGERING_OFFSET_DELTA = 1;

	@Inject
	private ClientEnvironment m_clientEnv;

	private long m_brokerSenderAcceptTimeout = DEFAULT_BROKER_SENDER_ACCEPT_TIMEOUT;

	private long m_brokerSenderResultTimeout = DEFAULT_BROKER_SENDER_RESULT_TIMEOUT;

	private int m_ProducerCallbackThreadCount = DEFAULT_PRODUCER_CALLBACK_THREAD_COUNT;

	private int m_brokerSenderTaskQueueSize = DEFAULT_BROKER_SENDER_TASK_QUEUE_SIZE;

	private int m_brokerSenderBatchSize = DEFAULT_BROKER_SENDER_BATCH_SIZE;

	private boolean m_logEnrichInfoEnabled = false;

	private boolean m_catEnabled = true;

	private int m_brokerSenderConcurrentLevel = DEFAULT_BROKER_SENDER_CONCURRENT_LEVEL;

	private int m_messageSenderSelectorSafeTriggerMinFireIntervalMillis = 50;

	private int m_messageSenderSelectorSafeTriggerIntervalMillis = 50;

	private Map<String, Integer> m_messageSenderSelectorNormalTriggeringOffsetDeltas = new HashMap<>();

	private Map<String, Integer> m_messageSenderSelectorSafeTriggerTriggeringOffsetDeltas = new HashMap<>();

	@Override
	public void initialize() throws InitializationException {

		String brokerSenderTaskQueueSizeStr = m_clientEnv.getGlobalConfig().getProperty("producer.sender.taskqueue.size");
		if (StringUtils.isNumeric(brokerSenderTaskQueueSizeStr)) {
			m_brokerSenderTaskQueueSize = Integer.valueOf(brokerSenderTaskQueueSizeStr);
		}

		String producerCallbackThreadCountStr = m_clientEnv.getGlobalConfig()
		      .getProperty("producer.callback.threadcount");
		if (StringUtils.isNumeric(producerCallbackThreadCountStr)) {
			m_ProducerCallbackThreadCount = Integer.valueOf(producerCallbackThreadCountStr);
		}

		String brokerSenderBatchSizeStr = m_clientEnv.getGlobalConfig().getProperty("producer.sender.batchsize");
		if (StringUtils.isNumeric(brokerSenderBatchSizeStr)) {
			m_brokerSenderBatchSize = Integer.valueOf(brokerSenderBatchSizeStr);
		}

		// FIXME rename this ugly name
		String logEnrichInfoEnabledStr = m_clientEnv.getGlobalConfig().getProperty("logEnrichInfo", "false");
		if ("true".equalsIgnoreCase(logEnrichInfoEnabledStr)) {
			m_logEnrichInfoEnabled = true;
		}

		String catEnabled = m_clientEnv.getGlobalConfig().getProperty("producer.cat.enable");
		if (!StringUtils.isBlank(catEnabled)) {
			m_catEnabled = !"false".equalsIgnoreCase(catEnabled);
		}

		String senderConcurrentLevelStr = m_clientEnv.getGlobalConfig().getProperty("producer.concurrent.level");
		if (StringUtils.isNumeric(senderConcurrentLevelStr)) {
			m_brokerSenderConcurrentLevel = Integer.valueOf(senderConcurrentLevelStr);
		}
		String brokerSenderAcceptTimeout = m_clientEnv.getGlobalConfig().getProperty(
		      "producer.sender.accept.timeout.millis");
		if (StringUtils.isNumeric(brokerSenderAcceptTimeout)) {
			m_brokerSenderAcceptTimeout = Integer.valueOf(brokerSenderAcceptTimeout);
		}
		String brokerSenderResultTimeout = m_clientEnv.getGlobalConfig().getProperty(
		      "producer.sender.result.timeout.millis");
		if (StringUtils.isNumeric(brokerSenderResultTimeout)) {
			m_brokerSenderResultTimeout = Integer.valueOf(brokerSenderResultTimeout);
		}

		String messageSenderSelectorSafeTriggerMinFireIntervalMillisStr = m_clientEnv.getGlobalConfig().getProperty(
		      "producer.message.sender.selector.safe.trigger.min.fire.interval.millis");
		if (StringUtils.isNumeric(messageSenderSelectorSafeTriggerMinFireIntervalMillisStr)) {
			m_messageSenderSelectorSafeTriggerMinFireIntervalMillis = Integer
			      .valueOf(messageSenderSelectorSafeTriggerMinFireIntervalMillisStr);
		}

		String messageSenderSelectorSafeTriggerIntervalMillisStr = m_clientEnv.getGlobalConfig().getProperty(
		      "producer.message.sender.selector.safe.trigger.interval.millis");
		if (StringUtils.isNumeric(messageSenderSelectorSafeTriggerIntervalMillisStr)) {
			m_messageSenderSelectorSafeTriggerIntervalMillis = Integer
			      .valueOf(messageSenderSelectorSafeTriggerIntervalMillisStr);
		}

		String messageSenderSelectorNormalTriggeringOffsetDeltas = m_clientEnv.getGlobalConfig().getProperty(
		      "producer.message.sender.selector.normal.triggering.offset.deltas");
		if (!StringUtils.isEmpty(messageSenderSelectorNormalTriggeringOffsetDeltas)) {
			m_messageSenderSelectorNormalTriggeringOffsetDeltas = JSON.parseObject(
			      messageSenderSelectorNormalTriggeringOffsetDeltas, new TypeReference<Map<String, Integer>>() {
			      });
		}

		String messageSenderSelectorSafeTriggerTriggeringOffsetDeltas = m_clientEnv.getGlobalConfig().getProperty(
		      "producer.message.sender.selector.safe.trigger.triggering.offset.deltas");
		if (!StringUtils.isEmpty(messageSenderSelectorSafeTriggerTriggeringOffsetDeltas)) {
			m_messageSenderSelectorSafeTriggerTriggeringOffsetDeltas = JSON.parseObject(
			      messageSenderSelectorSafeTriggerTriggeringOffsetDeltas, new TypeReference<Map<String, Integer>>() {
			      });
		}

		// FIXME log config loading details.
	}

	public int getProduceTimeoutSeconds(String topic) {
		try {
			String produceTimeout = m_clientEnv.getProducerConfig(topic).getProperty("produce.timeout.seconds");
			if (StringUtils.isNumeric(produceTimeout)) {
				return Integer.valueOf(produceTimeout);
			}
		} catch (Exception e) {
			// ignore
		}
		return -1;
	}

	public boolean isCatEnabled() {
		return m_catEnabled;
	}

	public int getBrokerSenderBatchSize() {
		return m_brokerSenderBatchSize;
	}

	public long getBrokerSenderAcceptTimeoutMillis() {
		return m_brokerSenderAcceptTimeout;
	}

	public int getBrokerSenderTaskQueueSize() {
		return m_brokerSenderTaskQueueSize;
	}

	public int getProducerCallbackThreadCount() {
		return m_ProducerCallbackThreadCount;
	}

	public long getBrokerSenderResultTimeoutMillis() {
		return m_brokerSenderResultTimeout;
	}

	public boolean isLogEnrichInfoEnabled() {
		return m_logEnrichInfoEnabled;
	}

	public int getBrokerSenderConcurrentLevel() {
		return m_brokerSenderConcurrentLevel;
	}

	public int getMessageSenderSelectorSafeTriggerMinFireIntervalMillis() {
		return m_messageSenderSelectorSafeTriggerMinFireIntervalMillis;
	}

	public int getMessageSenderSelectorSafeTriggerIntervalMillis() {
		return m_messageSenderSelectorSafeTriggerIntervalMillis;
	}

	public int getMessageSenderSelectorNormalTriggeringOffsetDelta(String topic) {
		Integer delta = m_messageSenderSelectorNormalTriggeringOffsetDeltas.get(topic);
		if (delta != null && delta > 0) {
			return delta;
		} else {
			return DEFAULT_MESSAGE_SENDER_SELECTOR_NORMAL_TRIGGERING_OFFSET_DELTA;
		}
	}

	public long getMessageSenderSelectorSafeTriggerTriggeringOffsetDelta(String topic) {
		Integer delta = m_messageSenderSelectorSafeTriggerTriggeringOffsetDeltas.get(topic);
		if (delta != null && delta > 0) {
			return delta;
		} else {
			return DEFAULT_MESSAGE_SENDER_SELECTOR_SAFE_TRIGGER_TRIGGERING_OFFSET_DELTA;
		}
	}

}
