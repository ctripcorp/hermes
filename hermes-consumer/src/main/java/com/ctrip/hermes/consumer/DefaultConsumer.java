package com.ctrip.hermes.consumer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.consumer.api.BaseMessageListener;
import com.ctrip.hermes.consumer.api.Consumer;
import com.ctrip.hermes.consumer.api.MessageListener;
import com.ctrip.hermes.consumer.api.MessageListenerConfig;
import com.ctrip.hermes.consumer.api.OffsetStorage;
import com.ctrip.hermes.consumer.api.PullConsumerConfig;
import com.ctrip.hermes.consumer.api.PullConsumerHolder;
import com.ctrip.hermes.consumer.engine.CompositeSubscribeHandle;
import com.ctrip.hermes.consumer.engine.Engine;
import com.ctrip.hermes.consumer.engine.SubscribeHandle;
import com.ctrip.hermes.consumer.engine.Subscriber;
import com.ctrip.hermes.consumer.engine.ack.AckManager;
import com.ctrip.hermes.consumer.engine.config.ConsumerConfig;
import com.ctrip.hermes.consumer.pull.DefaultPullConsumerHolder;
import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Topic;

@Named(type = com.ctrip.hermes.consumer.api.Consumer.class)
public class DefaultConsumer extends Consumer {

	private static final MessageListenerConfig DEFAULT_MESSAGE_LISTENER_CONFIG = new MessageListenerConfig();

	@Inject
	private Engine m_engine;

	@Inject
	private MetaService m_metaService;

	@Inject
	private ConsumerConfig m_config;

	private ConsumerHolder start(String topicPattern, String groupId, MessageListener<?> listener,
	      MessageListenerConfig listenerConfig, ConsumerType consumerType) {
		return start(topicPattern, groupId, listener, listenerConfig, consumerType, null);
	}

	private ConsumerHolder start(String topicPattern, String groupId, MessageListener<?> listener,
	      MessageListenerConfig listenerConfig, ConsumerType consumerType, OffsetStorage offsetStorage) {
		return start(topicPattern, groupId, listener, listenerConfig, consumerType, offsetStorage, null);
	}

	private ConsumerHolder start(String topicPattern, String groupId, MessageListener<?> listener,
	      MessageListenerConfig listenerConfig, ConsumerType consumerType, OffsetStorage offsetStorage,
	      Class<?> messageClazz) {
		if (listener instanceof BaseMessageListener) {
			((BaseMessageListener<?>) listener).setGroupId(groupId);
		}
		SubscribeHandle subscribeHandle = m_engine.start(new Subscriber(topicPattern, groupId, listener, listenerConfig,
		      consumerType, offsetStorage, messageClazz));

		return new DefaultConsumerHolder(subscribeHandle);
	}

	@Override
	public ConsumerHolder start(String topic, String groupId, MessageListener<?> listener) {
		return start(topic, groupId, listener, DEFAULT_MESSAGE_LISTENER_CONFIG);
	}

	@Override
	public ConsumerHolder start(String topic, String groupId, MessageListener<?> listener, MessageListenerConfig config) {
		ConsumerType type = config.isStrictlyOrdering() ? ConsumerType.STRICTLY_ORDERING : ConsumerType.DEFAULT;
		return start(topic, groupId, listener, config, type);
	}

	public static class DefaultConsumerHolder implements ConsumerHolder {

		private SubscribeHandle m_subscribeHandle;

		public DefaultConsumerHolder(SubscribeHandle subscribeHandle) {
			m_subscribeHandle = subscribeHandle;
		}

		public boolean isConsuming() {
			if (m_subscribeHandle instanceof CompositeSubscribeHandle) {
				return ((CompositeSubscribeHandle) m_subscribeHandle).getChildHandleList().size() > 0;
			}
			return false;
		}

		@Override
		public void close() {
			m_subscribeHandle.close();
		}

	}

	@Override
	public Offset getOffsetByTime(String topicName, int partition, long time) {
		Topic topic = m_metaService.findTopicByName(topicName);
		if (topic == null || topic.findPartition(partition) == null) {
			throw new IllegalArgumentException(String.format("Topic [%s] or partition [%s] not found.", topicName,
			      partition));
		}
		return m_metaService.findMessageOffsetByTime(topicName, partition, time);
	}

	@Override
	public Map<Integer, Offset> getOffsetByTime(String topicName, long time) {
		Topic topic = m_metaService.findTopicByName(topicName);
		if (topic == null) {
			throw new IllegalArgumentException(String.format("Topic [%s] not found.", topicName));
		}

		return m_metaService.findMessageOffsetByTime(topicName, time);
	}

	private Topic validateTopicAndConsumerGroup(String topicName, String groupId) {
		Topic topic = m_metaService.findTopicByName(topicName);

		if (topic == null) {
			throw new IllegalArgumentException("Topic not found: " + topicName);
		}
		if (topic.findConsumerGroup(groupId) == null) {
			throw new IllegalArgumentException("Consumer group not found: " + groupId);
		}
		return topic;
	}

	@Override
	public <T> PullConsumerHolder<T> openPullConsumer(String topicName, String groupId, Class<T> messageClass,
	      PullConsumerConfig config) {
		return openPullConsumer(topicName, groupId, -1L, messageClass, config);
	}

	public <T> PullConsumerHolder<T> openPullConsumer(String topicName, String groupId, final long startTimeMillis,
	      Class<T> messageClass, PullConsumerConfig config) {

		ensureSingleTopic(topicName);

		Topic topic = validateTopicAndConsumerGroup(topicName, groupId);

		OffsetStorage offsetStorage = null;

		if (startTimeMillis > 0) {
			offsetStorage = new OffsetStorage() {
				ConcurrentMap<Pair<String, Integer>, Offset> m_offsets = new ConcurrentHashMap<>();

				@Override
				public Offset queryLatestOffset(String topic, int partition) {
					Pair<String, Integer> key = new Pair<String, Integer>(topic, partition);
					Offset offset = m_offsets.get(key);
					if (offset != null) {
						return offset;
					} else {
						return getOffsetByTime(topic, partition, startTimeMillis);
					}
				}

				@Override
				public void updatePulledOffset(String topic, int partition, Offset offset) {
					Pair<String, Integer> key = new Pair<String, Integer>(topic, partition);
					Offset existingOffset = m_offsets.get(key);
					if (existingOffset == null) {
						m_offsets.putIfAbsent(key, new Offset(0L, 0L, null));
						existingOffset = m_offsets.get(key);
					}
					synchronized (this) {
						if (existingOffset.getPriorityOffset() < offset.getPriorityOffset()) {
							existingOffset.setPriorityOffset(offset.getPriorityOffset());
						}
						if (existingOffset.getNonPriorityOffset() < offset.getNonPriorityOffset()) {
							existingOffset.setNonPriorityOffset(offset.getNonPriorityOffset());
						}
					}
				}
			};
		}

		DefaultPullConsumerHolder<T> holder = new DefaultPullConsumerHolder<T>(topicName, groupId, topic.getPartitions()
		      .size(), config, PlexusComponentLocator.lookup(AckManager.class), offsetStorage, m_config);

		MessageListenerConfig listenerConfig = new MessageListenerConfig();

		ConsumerHolder consumerHolder = start(topicName, groupId, holder, listenerConfig, ConsumerType.PULL,
		      offsetStorage, messageClass);
		holder.setConsumerHolder(consumerHolder);

		return holder;
	}

	private void ensureSingleTopic(String topicName) {
		if (topicName != null && (topicName.indexOf("*") >= 0 || topicName.indexOf("#") >= 0)) {
			throw new IllegalArgumentException("Pull consumer can not use topic pattern: " + topicName);
		}

	}
}
