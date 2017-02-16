package com.ctrip.hermes.metaserver.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaserver.meta.MetaHolder;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = ConsumerLeaseAllocatorLocator.class)
public class DefaultConsumerLeaseAllocatorLocator implements ConsumerLeaseAllocatorLocator {

	private static final Logger log = LoggerFactory.getLogger(DefaultConsumerLeaseAllocatorLocator.class);

	@Inject
	private MetaHolder m_metaHolder;

	@Inject
	private ConsumerLeaseAllocator m_consumeLeaseAllocator;

	@Override
	public ConsumerLeaseAllocator findAllocator(String topicName, String consumerGroupName) {
		ConsumerGroup consumerGroup = getConsumerGroup(topicName, consumerGroupName);
		if (consumerGroup != null) {
			return m_consumeLeaseAllocator;
		} else if (consumerGroup == null) {
			log.warn("ConsumerGroup {} not found for topic {}", topicName, consumerGroupName);
		}
		return null;
	}

	private ConsumerGroup getConsumerGroup(String topicName, String consumerGroupName) {
		Topic topic = m_metaHolder.getMeta().findTopic(topicName);
		if (topic != null) {
			return topic.findConsumerGroup(consumerGroupName);
		}

		return null;
	}

}
