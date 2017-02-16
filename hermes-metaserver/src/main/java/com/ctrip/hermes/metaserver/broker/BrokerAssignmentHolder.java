package com.ctrip.hermes.metaserver.broker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Idc;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaserver.commons.Assignment;
import com.ctrip.hermes.metaserver.commons.ClientContext;
import com.ctrip.hermes.metaserver.log.LoggerConstants;
import com.ctrip.hermes.metaserver.meta.MetaHolder;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = BrokerAssignmentHolder.class)
public class BrokerAssignmentHolder {

	private static final Logger log = LoggerFactory.getLogger(BrokerAssignmentHolder.class);

	private static final Logger traceLog = LoggerFactory.getLogger(LoggerConstants.TRACE);

	@Inject
	private BrokerPartitionAssigningStrategy m_brokerAssigningStrategy;

	private AtomicReference<Map<String, Assignment<Integer>>> m_assignments = new AtomicReference<>();

	private AtomicReference<Map<String, List<Topic>>> m_topicsCache = new AtomicReference<>();

	private AtomicReference<Map<String, Map<String, ClientContext>>> m_mergedBrokers;

	private AtomicReference<Map<Pair<String, Integer>, Endpoint>> m_configedBrokers;

	private AtomicReference<Map<String, ClientContext>> m_runningBrokers;

	@Inject
	private MetaHolder m_metaHolder;

	public BrokerAssignmentHolder() {
		m_assignments.set(new HashMap<String, Assignment<Integer>>());

		m_mergedBrokers = new AtomicReference<>();
		m_mergedBrokers.set(new HashMap<String, Map<String, ClientContext>>());

		m_configedBrokers = new AtomicReference<>();
		m_configedBrokers.set(new HashMap<Pair<String, Integer>, Endpoint>());

		m_runningBrokers = new AtomicReference<>();
		m_runningBrokers.set(new HashMap<String, ClientContext>());
	}

	private Map<String, ClientContext> getBrokers(String brokerGroup) {
		Map<String, Map<String, ClientContext>> allBrokers = m_mergedBrokers.get();
		if (allBrokers != null) {
			return allBrokers.get(brokerGroup);
		}
		return null;
	}

	private void setConfigedBrokers(List<Endpoint> endpoints) {
		if (endpoints != null) {
			Map<Pair<String, Integer>, Endpoint> configedBrokers = new HashMap<Pair<String, Integer>, Endpoint>(
			      endpoints.size());
			for (Endpoint endpoint : endpoints) {
				if (Endpoint.BROKER.equals(endpoint.getType())) {
					configedBrokers.put(new Pair<String, Integer>(endpoint.getHost(), endpoint.getPort()), endpoint);
				}
			}

			m_configedBrokers.set(configedBrokers);
		}
	}

	private void setRunningBrokers(Map<String, ClientContext> runningBrokers) {
		if (runningBrokers != null) {
			m_runningBrokers.set(new HashMap<String, ClientContext>(runningBrokers));
		}
	}

	private void mergeAvailableBrokers(Map<String, Idc> idcs) {
		Map<String, Map<String, ClientContext>> aggregateBrokers = new HashMap<String, Map<String, ClientContext>>();
		Map<String, ClientContext> runningBrokers = m_runningBrokers.get();
		Map<Pair<String, Integer>, Endpoint> configedBrokers = m_configedBrokers.get();

		if (runningBrokers != null && !runningBrokers.isEmpty() && configedBrokers != null && !configedBrokers.isEmpty()
		      && idcs != null && !idcs.isEmpty()) {
			for (Map.Entry<String, ClientContext> entry : runningBrokers.entrySet()) {
				ClientContext currentBroker = entry.getValue();
				if (currentBroker != null) {
					Pair<String, Integer> ipPort = new Pair<>(currentBroker.getIp(), currentBroker.getPort());
					Endpoint configedBroker = configedBrokers.get(ipPort);
					if (configedBroker != null) {
						Idc idc = idcs.get(configedBroker.getIdc());
						if (idc != null && idc.isEnabled() && configedBroker.isEnabled()) {
							String brokerGroup = configedBroker.getGroup();
							if (!aggregateBrokers.containsKey(brokerGroup)) {
								aggregateBrokers.put(brokerGroup, new HashMap<String, ClientContext>());
							}

							ClientContext existedBroker = aggregateBrokers.get(brokerGroup).get(currentBroker.getName());
							if (existedBroker == null
							      || existedBroker.getLastHeartbeatTime() < currentBroker.getLastHeartbeatTime()) {
								currentBroker.setGroup(brokerGroup);
								currentBroker.setIdc(configedBroker.getIdc());
								aggregateBrokers.get(brokerGroup).put(currentBroker.getName(), currentBroker);
							}
						}
					}
				}
			}
		}

		m_mergedBrokers.set(aggregateBrokers);
	}

	public Assignment<Integer> getAssignment(String topic) {
		return m_assignments.get().get(topic);
	}

	public AtomicReference<Map<String, Map<String, ClientContext>>> getMergedBrokers() {
		return m_mergedBrokers;
	}

	public AtomicReference<Map<Pair<String, Integer>, Endpoint>> getConfigedBrokers() {
		return m_configedBrokers;
	}

	public AtomicReference<Map<String, ClientContext>> getRunningBrokers() {
		return m_runningBrokers;
	}

	public Map<String, Assignment<Integer>> getAssignments() {
		return m_assignments.get();
	}

	public void reassign(Map<String, ClientContext> runningBrokers, Map<String, Idc> idcs) {
		reassign(runningBrokers, null, null, idcs);
	}

	public void reassign(List<Endpoint> configedBrokers, List<Topic> topics, Map<String, Idc> idcs) {
		reassign(null, configedBrokers, topics, idcs);
	}

	public void reassign(Map<String, ClientContext> runningBrokers, List<Endpoint> configedBrokers, List<Topic> topics,
	      Map<String, Idc> idcs) {
		if (runningBrokers != null) {
			setRunningBrokers(runningBrokers);
		}

		if (configedBrokers != null) {
			setConfigedBrokers(configedBrokers);
		}

		mergeAvailableBrokers(idcs);

		if (topics != null) {
			m_topicsCache.set(groupTopics(topics));
		}

		Map<String, Assignment<Integer>> newAssignments = assign();
		setAssignments(newAssignments);

		if (traceLog.isInfoEnabled()) {
			traceLog.info("Broker assignment changed.\n{}", JSON.toJSONString(newAssignments));
		}
	}

	private Map<String, Assignment<Integer>> assign() {
		Map<String, Assignment<Integer>> newAssignments = new HashMap<>();
		Map<String, List<Topic>> topics = m_topicsCache.get();
		if (topics != null) {
			for (Map.Entry<String, List<Topic>> entry : topics.entrySet()) {
				String brokerGroup = entry.getKey();
				List<Topic> groupTopics = entry.getValue();
				Map<String, ClientContext> groupBrokers = getBrokers(brokerGroup);

				if (groupTopics != null && !groupTopics.isEmpty()) {
					Map<String, Assignment<Integer>> groupAssignments = m_brokerAssigningStrategy.assign(groupBrokers,
					      groupTopics, getAssignments());
					newAssignments.putAll(groupAssignments);
				}
			}
		}
		return newAssignments;
	}

	private Map<String, List<Topic>> groupTopics(List<Topic> topics) {
		Map<String, List<Topic>> topicGroups = new HashMap<>();

		for (Topic topic : topics) {
			if (!StringUtils.isBlank(topic.getBrokerGroup())) {
				if (!topicGroups.containsKey(topic.getBrokerGroup())) {
					topicGroups.put(topic.getBrokerGroup(), new ArrayList<Topic>());
				}

				topicGroups.get(topic.getBrokerGroup()).add(topic);
			} else {
				log.warn("Topic({}) does not have broker group, will ignore it.", topic.getName());
			}
		}

		return topicGroups;
	}

	private void setAssignments(Map<String, Assignment<Integer>> newAssignments) {
		if (newAssignments != null) {
			m_assignments.set(newAssignments);
		}
	}

	public void clear() {
		setAssignments(new HashMap<String, Assignment<Integer>>());
		m_topicsCache.set(new HashMap<String, List<Topic>>());
		m_mergedBrokers.set(new HashMap<String, Map<String, ClientContext>>());
		m_configedBrokers.set(new HashMap<Pair<String, Integer>, Endpoint>());
		m_runningBrokers.set(new HashMap<String, ClientContext>());
	}

}
