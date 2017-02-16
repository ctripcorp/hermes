package com.ctrip.hermes.producer.status;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import org.unidal.tuple.Pair;

import com.ctrip.framework.vi.component.ComponentManager;
import com.ctrip.framework.vi.metrics.MetricsCollector;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public enum ProducerStatusMonitor {
	INSTANCE;

	private Map<Pair<String, Integer>, BlockingQueue<?>> m_taskQueues = new ConcurrentHashMap<>();

	private ProducerStatusMonitor() {
		registerVIComponents();
	}

	private static void registerVIComponents() {
		ComponentManager.register(ProducerTaskQueueComponentStatus.class);
		ComponentManager.register(ProducerConfigComponentStatus.class);
	}

	public void offerFailed(String topic, int partition) {
		MetricsCollector.getCollector().record("producer.offer.fail", createTagsMap(topic, partition));
	}

	public void brokerRejected(String topic, int partition, int messageCount) {
		MetricsCollector.getCollector().record("producer.broker.reject", createTagsMap(topic, partition));
	}

	public void brokerAcceptTimeout(String topic, int partition, int messageCount) {
		MetricsCollector.getCollector().record("producer.broker.accept.timeout", createTagsMap(topic, partition));
	}

	public void sendFail(String topic, int partition, int messageCount) {
		Map<String, String> tagsMap = createTagsMap(topic, partition);
		for (int i = 0; i < messageCount; i++) {
			MetricsCollector.getCollector().record("producer.send.fail", tagsMap);
		}
	}

	public void sendSuccess(String topic, int partition, long duration) {
		MetricsCollector.getCollector().record("producer.send.success", duration, createTagsMap(topic, partition));
	}

	public void sendSkip(String topic, int partition, int messageCount) {
		Map<String, String> tagsMap = createTagsMap(topic, partition);
		for (int i = 0; i < messageCount; i++) {
			MetricsCollector.getCollector().record("producer.send.skip", tagsMap);
		}
	}

	private Map<String, String> createTagsMap(String topic, int partition) {
		Map<String, String> tags = new HashMap<>(8);
		tags.put("topic", topic);
		tags.put("partition", Integer.toString(partition));

		return tags;
	}

	public void watchTaskQueue(String topic, int partition, BlockingQueue<?> queue) {
		m_taskQueues.put(new Pair<String, Integer>(topic, partition), queue);
	}

	public Set<Map.Entry<Pair<String, Integer>, BlockingQueue<?>>> listTaskQueues() {
		return m_taskQueues.entrySet();
	}
}
