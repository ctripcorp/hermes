package com.ctrip.hermes.consumer.engine;

import java.util.UUID;

import com.ctrip.hermes.consumer.ConsumerType;
import com.ctrip.hermes.consumer.api.MessageListener;
import com.ctrip.hermes.consumer.api.MessageListenerConfig;
import com.ctrip.hermes.consumer.api.OffsetStorage;
import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Topic;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@SuppressWarnings("rawtypes")
public class ConsumerContext {
	private Topic m_topic;

	private ConsumerGroup m_group;

	private Class<?> m_messageClazz;

	private MessageListener m_consumer;

	private ConsumerType m_consumerType;

	private MessageListenerConfig m_messageListenerConfig;

	private String m_sessionId;

	private OffsetStorage m_offsetStorage;

	public ConsumerContext(Topic topic, ConsumerGroup group, MessageListener consumer, Class<?> messageClazz,
	      ConsumerType consumerType, MessageListenerConfig messageListenerConfig, OffsetStorage offsetStorage) {
		m_topic = topic;
		m_group = group;
		m_consumer = consumer;
		m_messageClazz = messageClazz;
		m_consumerType = consumerType;
		m_messageListenerConfig = messageListenerConfig;
		m_offsetStorage = offsetStorage;
		m_sessionId = System.getProperty("consumerSessionId", UUID.randomUUID().toString());
	}

	public String getSessionId() {
		return m_sessionId;
	}

	public ConsumerType getConsumerType() {
		return m_consumerType;
	}

	public Class<?> getMessageClazz() {
		return m_messageClazz;
	}

	public Topic getTopic() {
		return m_topic;
	}

	public ConsumerGroup getGroup() {
		return m_group;
	}

	public String getGroupId() {
		return m_group.getName();
	}

	public MessageListener getConsumer() {
		return m_consumer;
	}

	public MessageListenerConfig getMessageListenerConfig() {
		return m_messageListenerConfig;
	}

	public OffsetStorage getOffsetStorage() {
		return m_offsetStorage;
	}

	@Override
	public String toString() {
		return "ConsumerContext [m_topic=" + m_topic.getName() + ", m_groupId=" + m_group.getName() + ", m_messageClazz="
		      + m_messageClazz + ", m_consumer=" + m_consumer + ", m_consumerType=" + m_consumerType + "]";
	}

}
