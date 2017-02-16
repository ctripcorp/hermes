package com.ctrip.hermes.metaserver.meta;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.meta.entity.Server;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaserver.assign.AssignBalancer;
import com.ctrip.hermes.metaserver.commons.Assignment;
import com.ctrip.hermes.metaserver.commons.ClientContext;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = MetaServerAssigningStrategy.class)
public class LeastAdjustmentMetaServerAssigningStrategy implements MetaServerAssigningStrategy {

	private final static Logger log = LoggerFactory.getLogger(LeastAdjustmentMetaServerAssigningStrategy.class);

	@Inject
	private AssignBalancer m_assignBalancer;

	@Override
	public Assignment<String> assign(List<Server> metaServers, List<Topic> topics, Assignment<String> originAssignments) {
		Assignment<String> newAssignments = new Assignment<>();

		if (metaServers == null || metaServers.isEmpty() || topics == null || topics.isEmpty()) {
			return newAssignments;
		}

		if (originAssignments == null) {
			originAssignments = new Assignment<>();
		}

		Set<String> topicNames = new HashSet<>();
		for (Topic topic : topics) {
			topicNames.add(topic.getName());
		}

		Map<String, Server> currentMetaServers = new HashMap<>();
		for (Server server : metaServers) {
			currentMetaServers.put(server.getId(), server);
		}

		Map<String, List<String>> originMetaServerToTopic = mapMetaServerToTopics(topicNames, currentMetaServers,
		      originAssignments);
		List<String> freeTopics = findFreeTopics(topics, originMetaServerToTopic);
		Map<String, List<String>> newAssins = m_assignBalancer.assign(originMetaServerToTopic, freeTopics);
		for (Entry<String, List<String>> entry : newAssins.entrySet()) {
			putAssignToResult(newAssignments, currentMetaServers, entry.getKey(), entry.getValue());
		}

		return newAssignments;
	}

	private List<String> findFreeTopics(List<Topic> topics, Map<String, List<String>> originMetaServerToTopic) {
		List<String> freeTopics = new ArrayList<>();
		Set<String> originTopics = new HashSet<>();
		
		for (List<String> ts : originMetaServerToTopic.values()) {
			originTopics.addAll(ts);
      }
		
		for (Topic topic : topics) {
			if (!originTopics.contains(topic.getName())) {
				freeTopics.add(topic.getName());
			}
		}
		return freeTopics;
	}

	private void putAssignToResult(Assignment<String> newAssignments, Map<String, Server> currentMetaServers,
	      String metaServerName, List<String> newAssign) {
		for (String topic : newAssign) {
			Map<String, ClientContext> server = new HashMap<>();
			Server metaServer = currentMetaServers.get(metaServerName);
			server.put(metaServerName, new ClientContext(metaServer.getId(), metaServer.getHost(), metaServer.getPort(),
			      null, metaServer.getIdc(), -1));
			newAssignments.addAssignment(topic, server);
		}
	}

	private Map<String, List<String>> mapMetaServerToTopics(Set<String> currentTopics,
	      Map<String, Server> currentMetaServers, Assignment<String> originAssignments) {
		Map<String, List<String>> result = new HashMap<>();
		for (Entry<String, Server> server : currentMetaServers.entrySet()) {
			result.put(server.getKey(), new ArrayList<String>());
		}

		Set<String> metaServerNames = currentMetaServers.keySet();

		for (Map.Entry<String, Map<String, ClientContext>> entry : originAssignments.getAssignments().entrySet()) {
			String topic = entry.getKey();
			if (entry.getValue().size() != 1) {
				log.warn("Topic {} have more than one metaServer assigned", topic);
			}

			String metaServer = entry.getValue().keySet().iterator().next();

			if (currentTopics.contains(topic) && metaServerNames.contains(metaServer)) {
				result.get(metaServer).add(topic);
			}
		}

		return result;
	}
}
