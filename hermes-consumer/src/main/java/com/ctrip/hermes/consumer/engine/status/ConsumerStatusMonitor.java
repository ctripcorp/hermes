package com.ctrip.hermes.consumer.engine.status;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import org.unidal.tuple.Triple;

import com.ctrip.framework.vi.component.ComponentManager;
import com.ctrip.framework.vi.metrics.MetricsCollector;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.message.ConsumerMessage;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public enum ConsumerStatusMonitor {
	INSTANCE;

	private Map<Triple<String, Integer, String>, BlockingQueue<?>> m_localcaches = new ConcurrentHashMap<>();

	private ConsumerStatusMonitor() {
		registerVIComponents();
	}

	private static void registerVIComponents() {
		ComponentManager.register(ConsumerLocalCacheComponentStatus.class);
		ComponentManager.register(ConsumerConfigComponentStatus.class);
	}

	public void msgReceived(Tpg tpg) {
		MetricsCollector.getCollector().record("msg.received",
		      createTagsMap(tpg.getTopic(), tpg.getPartition(), tpg.getGroupId()));
	}

	public void msgAcked(Tpg tpg, long onMessageStart, long onMessageEnd, boolean ack) {
		if (ack) {
			MetricsCollector.getCollector().record("msg.ack", onMessageEnd - onMessageStart,
			      createTagsMap(tpg.getTopic(), tpg.getPartition(), tpg.getGroupId()));
		} else {
			MetricsCollector.getCollector().record("msg.nack", onMessageEnd - onMessageStart,
			      createTagsMap(tpg.getTopic(), tpg.getPartition(), tpg.getGroupId()));
		}
	}

	public void watchLocalCache(String topic, int partitionId, String groupId, BlockingQueue<ConsumerMessage<?>> msgs) {
		m_localcaches.put(new Triple<>(topic, partitionId, groupId), msgs);
	}

	public void stopWatchLocalCache(String topic, int partitionId, String groupId) {
		m_localcaches.remove(new Triple<>(topic, partitionId, groupId));
	}

	public void msgProcessed(String topic, int partition, String groupId, int msgCount, long duration) {
		for (int i = 0; i < msgCount; i++) {
			MetricsCollector.getCollector().record("msg.processed", duration, createTagsMap(topic, partition, groupId));
		}
	}

	private Map<String, String> createTagsMap(String topic, int partition, String group) {
		Map<String, String> tags = new HashMap<>(8);
		tags.put("topic", topic);
		tags.put("partition", Integer.toString(partition));
		tags.put("group", group);

		return tags;
	}

	public Set<Map.Entry<Triple<String, Integer, String>, BlockingQueue<?>>> listLocalCaches() {
		return m_localcaches.entrySet();
	}
}
