package com.ctrip.hermes.metaserver.consumer;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.metaserver.commons.ClientContext;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class ActiveConsumerList {
	private boolean m_changed = true;

	private Map<String, ClientContext> m_consumers = new HashMap<>();

	public void heartbeat(String consumerName, long heartbeatTime, String ip) {
		if (!m_consumers.containsKey(consumerName)) {
			m_changed = true;
			m_consumers.put(consumerName, new ClientContext(consumerName, ip, -1, null, null, heartbeatTime));
		} else {
			ClientContext consumerContext = m_consumers.get(consumerName);
			if (!StringUtils.equals(consumerContext.getIp(), ip)) {
				m_changed = true;
				consumerContext.setIp(ip);
			}
			consumerContext.setLastHeartbeatTime(heartbeatTime);
		}
	}

	public void purgeExpired(long timeoutMillis, long now) {
		Iterator<Entry<String, ClientContext>> iter = m_consumers.entrySet().iterator();
		while (iter.hasNext()) {
			Entry<String, ClientContext> entry = iter.next();
			if (entry.getValue().getLastHeartbeatTime() + timeoutMillis < now) {
				iter.remove();
				m_changed = true;
			}
		}
	}

	public boolean getAndResetChanged() {
		boolean changed = m_changed;
		m_changed = false;
		return changed;
	}

	public Map<String, ClientContext> getActiveConsumers() {
		return new HashMap<>(m_consumers);
	}

}
