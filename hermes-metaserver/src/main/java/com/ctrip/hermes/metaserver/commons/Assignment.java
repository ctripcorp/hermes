package com.ctrip.hermes.metaserver.commons;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class Assignment<Key> {
	private ConcurrentMap<Key, Map<String, ClientContext>> m_assignment = new ConcurrentHashMap<>();

	public boolean isAssignTo(Key key, String client) {
		Map<String, ClientContext> clients = m_assignment.get(key);
		return clients != null && !clients.isEmpty() && clients.keySet().contains(client);
	}

	public Map<String, ClientContext> getAssignment(Key key) {
		return m_assignment.get(key);
	}

	public void addAssignment(Key key, Map<String, ClientContext> clients) {
		if (!m_assignment.containsKey(key)) {
			m_assignment.putIfAbsent(key, new HashMap<String, ClientContext>());
		}
		m_assignment.get(key).putAll(clients);
	}

	public Map<Key, Map<String, ClientContext>> getAssignments() {
		return m_assignment;
	}

	@Override
	public String toString() {
		return "Assignment [m_assignment=" + m_assignment + "]";
	}

}
