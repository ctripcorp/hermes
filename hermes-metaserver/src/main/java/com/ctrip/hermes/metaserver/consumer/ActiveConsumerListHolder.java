package com.ctrip.hermes.metaserver.consumer;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.metaserver.commons.ClientContext;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = ActiveConsumerListHolder.class)
public class ActiveConsumerListHolder {
	@Inject
	private SystemClockService m_systemClockService;

	private Map<Pair<String, String>, ActiveConsumerList> m_activeConsumerLists = new HashMap<>();

	private ReentrantLock m_lock = new ReentrantLock();

	public void setSystemClockService(SystemClockService systemClockService) {
		m_systemClockService = systemClockService;
	}

	public void heartbeat(Pair<String, String> topicGroup, String consumerName, String ip) {
		m_lock.lock();
		try {
			if (!m_activeConsumerLists.containsKey(topicGroup)) {
				m_activeConsumerLists.put(topicGroup, new ActiveConsumerList());
			}
			ActiveConsumerList activeConsumerList = m_activeConsumerLists.get(topicGroup);
			activeConsumerList.heartbeat(consumerName, m_systemClockService.now(), ip);
		} finally {
			m_lock.unlock();
		}

	}

	public Map<Pair<String, String>, Map<String, ClientContext>> scanChanges(long timeout, TimeUnit timeUnit) {
		Map<Pair<String, String>, Map<String, ClientContext>> changes = new HashMap<>();
		long timeoutMillis = timeUnit.toMillis(timeout);
		m_lock.lock();
		try {
			Iterator<Entry<Pair<String, String>, ActiveConsumerList>> iter = m_activeConsumerLists.entrySet().iterator();

			while (iter.hasNext()) {
				Entry<Pair<String, String>, ActiveConsumerList> entry = iter.next();
				ActiveConsumerList activeConsumerList = entry.getValue();
				if (activeConsumerList != null) {
					activeConsumerList.purgeExpired(timeoutMillis, m_systemClockService.now());

					Map<String, ClientContext> activeConsumers = activeConsumerList.getActiveConsumers();

					if (activeConsumers == null || activeConsumers.isEmpty()) {
						iter.remove();
						changes.put(entry.getKey(), activeConsumers);
					} else {
						if (activeConsumerList.getAndResetChanged()) {
							changes.put(entry.getKey(), activeConsumers);
						}
					}
				} else {
					iter.remove();
				}
			}
		} finally {
			m_lock.unlock();
		}

		return changes;
	}

	public ActiveConsumerList getActiveConsumerList(Pair<String, String> topicGroup) {
		m_lock.lock();
		try {
			return m_activeConsumerLists.get(topicGroup);
		} finally {
			m_lock.unlock();
		}
	}

}
