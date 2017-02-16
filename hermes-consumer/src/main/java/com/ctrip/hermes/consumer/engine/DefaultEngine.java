package com.ctrip.hermes.consumer.engine;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.consumer.engine.bootstrap.ConsumerBootstrap;
import com.ctrip.hermes.consumer.engine.bootstrap.ConsumerBootstrapManager;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.utils.CollectionUtil;
import com.ctrip.hermes.core.utils.CollectionUtil.Transformer;
import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Topic;

@Named(type = Engine.class)
public class DefaultEngine extends Engine {

	private static final Logger log = LoggerFactory.getLogger(DefaultEngine.class);

	@Inject
	private ConsumerBootstrapManager m_consumerManager;

	@Inject
	private MetaService m_metaService;

	@Override
	public SubscribeHandle start(Subscriber subscriber) {
		CompositeSubscribeHandle handle = new CompositeSubscribeHandle();

		List<Topic> topics = m_metaService.listTopicsByPattern(subscriber.getTopicPattern());

		if (topics == null || topics.isEmpty()) {
			throw new IllegalArgumentException(String.format("Can not find any topics matching pattern %s",
			      subscriber.getTopicPattern()));
		}

		log.info("Found topics({}) matching pattern({}), groupId={}.", CollectionUtil.collect(topics, new Transformer() {

			@Override
			public Object transform(Object topic) {
				return ((Topic) topic).getName();
			}
		}), subscriber.getTopicPattern(), subscriber.getGroupId());

		validate(topics, subscriber.getGroupId());

		for (Topic topic : topics) {
			ConsumerGroup cg = topic.findConsumerGroup(subscriber.getGroupId());
			ConsumerContext context = new ConsumerContext(topic, cg, subscriber.getConsumer(),
			      subscriber.getMessageClass(), subscriber.getConsumerType(), subscriber.getMessageListenerConfig(),
			      subscriber.getOffsetStorage());

			try {
				ConsumerBootstrap consumerBootstrap = m_consumerManager.findConsumerBootStrap(topic);
				handle.addSubscribeHandle(consumerBootstrap.start(context));

			} catch (RuntimeException e) {
				log.error("Failed to start consumer for topic {} (consumer: groupId={}, sessionId={})", topic.getName(),
				      context.getGroupId(), context.getSessionId(), e);
				throw e;
			}
		}

		return handle;
	}

	private void validate(List<Topic> topics, String groupId) {
		List<String> failedTopics = new ArrayList<String>();
		boolean hasError = false;

		for (Topic topic : topics) {
			if (!m_metaService.containsConsumerGroup(topic.getName(), groupId)) {
				failedTopics.add(topic.getName());
				hasError = true;
			}
		}

		if (hasError) {
			throw new IllegalArgumentException(String.format(
			      "Consumer group %s not found for topics (%s), please add consumer group in Hermes-Portal first.",
			      groupId, failedTopics));
		}

	}
}
