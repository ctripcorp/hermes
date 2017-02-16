package com.ctrip.hermes.core.status;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import com.ctrip.framework.vi.component.ComponentManager;
import com.ctrip.framework.vi.metrics.MetricsCollector;
import com.ctrip.hermes.core.constants.CatConstants;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.utils.CatUtil;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public enum StatusMonitor {
	INSTANCE;

	private Map<String, BlockingQueue<?>> m_commandProcessorQueues = new ConcurrentHashMap<>();

	private StatusMonitor() {
		registerVIComponents();
	}

	private static void registerVIComponents() {
		ComponentManager.register(CommandProcessorQueueComponentStatus.class);
	}

	public void watchCommandProcessorQueue(String poolName, final BlockingQueue<Runnable> workQueue) {
		m_commandProcessorQueues.put(poolName, workQueue);
	}

	public void commandReceived(CommandType type, String clientIp) {
		Map<String, String> tags = createTagsMap(type);
		tags.put("remoteIp", clientIp);

		MetricsCollector.getCollector().record("commandReceived", tags);

		if (type != null) {
			CatUtil.logEventPeriodically(CatConstants.TYPE_HERMES_CMD_VERSION, type.toString() + "-RCV");
		}
	}

	private Map<String, String> createTagsMap(CommandType type) {
		Map<String, String> tags = new HashMap<>(8);
		tags.put("type", type.toString());

		return tags;
	}

	public void commandProcessorException(CommandType type, CommandProcessor processor) {
		MetricsCollector.getCollector().record("commandProcessor.exception", createTagsMap(type));
	}

	public void commandProcessed(CommandType type, long duration) {
		MetricsCollector.getCollector().record("commandProcessor.processed", duration, createTagsMap(type));
	}

	public Set<Map.Entry<String, BlockingQueue<?>>> listCommandProcessorQueues() {
		return m_commandProcessorQueues.entrySet();
	}

}
