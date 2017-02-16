package com.ctrip.hermes.consumer.api;

import java.util.Map;

import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;

public abstract class Consumer {

	public abstract ConsumerHolder start(String topic, String groupId, MessageListener<?> listener);

	public abstract ConsumerHolder start( //
	      String topicPattern, String groupId, MessageListener<?> listener, MessageListenerConfig config);

	public abstract <T> PullConsumerHolder<T> openPullConsumer(String topic, String groupId, Class<T> messageClass,
	      PullConsumerConfig config);

	public abstract Map<Integer, Offset> getOffsetByTime(String topic, long time);

	public abstract Offset getOffsetByTime(String topic, int partitionId, long time);

	public static Consumer getInstance() {
		return PlexusComponentLocator.lookup(Consumer.class);
	}

	public interface ConsumerHolder {
		void close();
	}

}
