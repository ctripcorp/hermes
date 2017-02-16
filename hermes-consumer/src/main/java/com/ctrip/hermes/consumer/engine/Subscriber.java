package com.ctrip.hermes.consumer.engine;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import com.ctrip.hermes.consumer.ConsumerType;
import com.ctrip.hermes.consumer.api.MessageListener;
import com.ctrip.hermes.consumer.api.MessageListenerConfig;
import com.ctrip.hermes.consumer.api.OffsetStorage;

@SuppressWarnings("rawtypes")
public class Subscriber {

	private final static MessageListenerConfig DEFAULT_MESSAGE_LISTENER_CONFIG = new MessageListenerConfig();

	private String m_groupId;

	private String m_topicPattern;

	private MessageListener m_consumer;

	private ConsumerType m_consumerType;

	private MessageListenerConfig m_messageListenerConfig;

	private OffsetStorage m_offsetStorage;

	private Class<?> m_messageClass;

	public Subscriber(String topicPattern, String groupId, MessageListener consumer,
	      MessageListenerConfig messageListenerConfig, ConsumerType consumerType, OffsetStorage offsetStorage,
	      Class<?> messageClass) {
		m_topicPattern = topicPattern;
		m_groupId = groupId;
		m_consumer = consumer;
		m_consumerType = consumerType;
		m_messageListenerConfig = messageListenerConfig;
		m_offsetStorage = offsetStorage;
		m_messageClass = messageClass;
	}

	public Subscriber(String topicPattern, String groupId, MessageListener consumer,
	      MessageListenerConfig listenerConfig, ConsumerType consumerType) {
		this(topicPattern, groupId, consumer, listenerConfig, consumerType, null, null);
	}

	public Subscriber(String topicPattern, String groupId, MessageListener consumer, ConsumerType consumerType,
	      OffsetStorage offsetStorage) {
		this(topicPattern, groupId, consumer, DEFAULT_MESSAGE_LISTENER_CONFIG, consumerType, offsetStorage, null);
	}

	public Subscriber(String topicPattern, String groupId, MessageListener consumer, ConsumerType consumerType) {
		this(topicPattern, groupId, consumer, DEFAULT_MESSAGE_LISTENER_CONFIG, consumerType, null, null);
	}

	public Subscriber(String topicPattern, String groupId, MessageListener consumer) {
		this(topicPattern, groupId, consumer, DEFAULT_MESSAGE_LISTENER_CONFIG, ConsumerType.DEFAULT, null, null);
	}

	public Subscriber(String topicPattern, String groupId, MessageListener consumer,
	      MessageListenerConfig messageListenerConfig) {
		this(topicPattern, groupId, consumer, messageListenerConfig, ConsumerType.DEFAULT, null, null);
	}

	public ConsumerType getConsumerType() {
		return m_consumerType;
	}

	public String getGroupId() {
		return m_groupId;
	}

	public String getTopicPattern() {
		return m_topicPattern;
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

	public Class<?> getMessageClass() {
		if (m_messageClass != null) {
			return m_messageClass;
		}
		Type genericSuperClass = m_consumer.getClass().getGenericSuperclass();
		if (genericSuperClass instanceof ParameterizedType) {
			ParameterizedType paraType = (ParameterizedType) genericSuperClass;
			Type[] actualTypeArguments = paraType.getActualTypeArguments();
			Class type = (Class) actualTypeArguments[0];
			return type;
		} else {
			Type[] genericInterfaces = m_consumer.getClass().getGenericInterfaces();
			Type[] actualTypeArguments = ((ParameterizedType) genericInterfaces[0]).getActualTypeArguments();
			Class type = (Class) actualTypeArguments[0];
			return type;
		}
	}
}
