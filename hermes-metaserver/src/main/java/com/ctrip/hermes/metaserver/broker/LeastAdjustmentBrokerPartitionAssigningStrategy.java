package com.ctrip.hermes.metaserver.broker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaserver.assign.AssignBalancer;
import com.ctrip.hermes.metaserver.commons.Assignment;
import com.ctrip.hermes.metaserver.commons.ClientContext;

@Named(type = BrokerPartitionAssigningStrategy.class)
public class LeastAdjustmentBrokerPartitionAssigningStrategy implements BrokerPartitionAssigningStrategy {

	private final static Logger log = LoggerFactory.getLogger(LeastAdjustmentBrokerPartitionAssigningStrategy.class);

	@Inject
	private AssignBalancer m_assignBalancer;

	@Override
	public Map<String, Assignment<Integer>> assign(Map<String, ClientContext> brokers, List<Topic> topics,
	      Map<String, Assignment<Integer>> originAssignments) {
		Map<String, Assignment<Integer>> newAssignments = new HashMap<String, Assignment<Integer>>();

		if (brokers == null || brokers.isEmpty() || topics == null || topics.isEmpty()) {
			return newAssignments;
		}

		Map<String, List<Pair<String, Integer>>> avalibleOriginAssignments = new HashMap<String, List<Pair<String, Integer>>>();
		List<Pair<String, Integer>> freeTps = new ArrayList<Pair<String, Integer>>();
		for (String broker : brokers.keySet()) {
			avalibleOriginAssignments.put(broker, new ArrayList<Pair<String, Integer>>());
		}

		for (Topic topic : topics) {
			if (originAssignments.containsKey(topic.getName())) {
				Assignment<Integer> topicOriginAssignments = originAssignments.get(topic.getName());
				for (Partition partition : topic.getPartitions()) {
					Map<String, ClientContext> originTPAssianment = topicOriginAssignments.getAssignment(partition.getId());
					if (originTPAssianment != null) {
						if (originTPAssianment.size() != 1) {
							log.error("TP have more than one broker assigned");
						}
						ClientContext broker = originTPAssianment.values().iterator().next();
						if (brokers.containsKey(broker.getName())) {
							avalibleOriginAssignments.get(broker.getName()).add(
							      new Pair<String, Integer>(topic.getName(), partition.getId()));
							continue;
						}
					}
					freeTps.add(new Pair<String, Integer>(topic.getName(), partition.getId()));
				}
			} else {
				for (Partition partition : topic.getPartitions()) {
					freeTps.add(new Pair<String, Integer>(topic.getName(), partition.getId()));
				}
			}
		}

		Map<String, List<Pair<String, Integer>>> newAssigns = m_assignBalancer.assign(avalibleOriginAssignments,
		      freeTps);

		// change from brokerPartitionMap to tpBrokerAssignments
		for (Entry<String, List<Pair<String, Integer>>> assign : newAssigns.entrySet()) {
			for (Pair<String, Integer> tp : assign.getValue()) {
				if (!newAssignments.containsKey(tp.getKey())) {
					newAssignments.put(tp.getKey(), new Assignment<Integer>());
				}
				Map<String, ClientContext> broker = new HashMap<String, ClientContext>();
				broker.put(assign.getKey(), brokers.get(assign.getKey()));
				newAssignments.get(tp.getKey()).addAssignment(tp.getValue(), broker);
			}
		}
		return newAssignments;
	}

}
