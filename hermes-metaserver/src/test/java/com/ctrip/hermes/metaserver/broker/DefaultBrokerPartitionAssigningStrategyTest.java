package com.ctrip.hermes.metaserver.broker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaserver.commons.Assignment;
import com.ctrip.hermes.metaserver.commons.ClientContext;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class DefaultBrokerPartitionAssigningStrategyTest {

	private BrokerPartitionAssigningStrategy m_strategy;

	@Before
	public void before() throws Exception {
		m_strategy = new DefaultBrokerPartitionAssigningStrategy();
	}

	@Test
	public void testBrokerAdded() throws Exception {
		long now = System.currentTimeMillis();
		Map<String, ClientContext> brokers = createBrokers(Arrays.asList(//
		      new ClientContext("br0", "0.0.0.0", 1234, null, null, now),//
		      new ClientContext("br1", "0.0.0.1", 1234, null, null, now)//
		      ));

		List<Topic> topics = new ArrayList<>();
		topics.add(createTopic("t1", 2, Endpoint.BROKER));
		topics.add(createTopic("t2", 2, Endpoint.KAFKA));
		topics.add(createTopic("t3", 2, Endpoint.BROKER));

		Map<String, Assignment<Integer>> originAssignments = createAssignments(Arrays.asList(//
		      new Pair<>(new Pair<>("t1", 0), new ClientContext("br0", "0.0.0.0", 1234, null, null, now)),//
		      new Pair<>(new Pair<>("t1", 1), new ClientContext("br0", "0.0.0.0", 1234, null, null, now)),//
		      new Pair<>(new Pair<>("t3", 0), new ClientContext("br0", "0.0.0.0", 1234, null, null, now)),//
		      new Pair<>(new Pair<>("t3", 1), new ClientContext("br0", "0.0.0.0", 1234, null, null, now))//

		      ));
		Map<String, Assignment<Integer>> assignments = m_strategy.assign(brokers, topics, originAssignments);

		assertAssignment(assignments,//
		      topics,//
		      Arrays.asList(//
		            new ClientContext("br0", "0.0.0.0", 1234, null, null, now),//
		            new ClientContext("br1", "0.0.0.1", 1234, null, null, now)//
		      ));
	}

	@Test
	public void testBrokerDeleted() throws Exception {
		long now = System.currentTimeMillis();
		Map<String, ClientContext> brokers = createBrokers(Arrays.asList(//
		      new ClientContext("br0", "0.0.0.0", 1234, null, null, now)//
		      ));

		List<Topic> topics = new ArrayList<>();
		topics.add(createTopic("t1", 2, Endpoint.BROKER));
		topics.add(createTopic("t2", 2, Endpoint.KAFKA));
		topics.add(createTopic("t3", 2, Endpoint.BROKER));

		Map<String, Assignment<Integer>> originAssignments = createAssignments(Arrays.asList(//
		      new Pair<>(new Pair<>("t1", 0), new ClientContext("br0", "0.0.0.0", 1234, null, null, now)),//
		      new Pair<>(new Pair<>("t1", 1), new ClientContext("br1", "0.0.0.1", 1234, null, null, now)),//
		      new Pair<>(new Pair<>("t3", 0), new ClientContext("br0", "0.0.0.0", 1234, null, null, now)),//
		      new Pair<>(new Pair<>("t3", 1), new ClientContext("br1", "0.0.0.1", 1234, null, null, now))//

		      ));
		Map<String, Assignment<Integer>> assignments = m_strategy.assign(brokers, topics, originAssignments);

		assertAssignment(assignments,//
		      topics,//
		      Arrays.asList(//
		      new ClientContext("br0", "0.0.0.0", 1234, null, null, now)//
		      ));
	}

	@Test
	public void testTopicAdded() throws Exception {
		long now = System.currentTimeMillis();
		Map<String, ClientContext> brokers = createBrokers(Arrays.asList(//
		      new ClientContext("br0", "0.0.0.0", 1234, null, null, now),//
		      new ClientContext("br1", "0.0.0.1", 1234, null, null, now)//
		      ));

		List<Topic> topics = new ArrayList<>();
		topics.add(createTopic("t1", 2, Endpoint.BROKER));
		topics.add(createTopic("t2", 2, Endpoint.KAFKA));
		topics.add(createTopic("t3", 2, Endpoint.BROKER));
		topics.add(createTopic("t4", 2, Endpoint.BROKER));

		Map<String, Assignment<Integer>> originAssignments = createAssignments(Arrays.asList(//
		      new Pair<>(new Pair<>("t1", 0), new ClientContext("br0", "0.0.0.0", 1234, null, null, now)),//
		      new Pair<>(new Pair<>("t1", 1), new ClientContext("br1", "0.0.0.1", 1234, null, null, now)),//
		      new Pair<>(new Pair<>("t3", 0), new ClientContext("br0", "0.0.0.0", 1234, null, null, now)),//
		      new Pair<>(new Pair<>("t3", 1), new ClientContext("br1", "0.0.0.1", 1234, null, null, now))//

		      ));
		Map<String, Assignment<Integer>> assignments = m_strategy.assign(brokers, topics, originAssignments);

		assertAssignment(assignments,//
		      topics,//
		      Arrays.asList(//
		            new ClientContext("br0", "0.0.0.0", 1234, null, null, now),//
		            new ClientContext("br1", "0.0.0.1", 1234, null, null, now)//
		      ));
	}

	@Test
	public void testTopicDeleted() throws Exception {
		long now = System.currentTimeMillis();
		Map<String, ClientContext> brokers = createBrokers(Arrays.asList(//
		      new ClientContext("br0", "0.0.0.0", 1234, null, null, now),//
		      new ClientContext("br1", "0.0.0.1", 1234, null, null, now)//
		      ));

		List<Topic> topics = new ArrayList<>();
		topics.add(createTopic("t1", 2, Endpoint.BROKER));
		topics.add(createTopic("t2", 2, Endpoint.KAFKA));

		Map<String, Assignment<Integer>> originAssignments = createAssignments(Arrays.asList(//
		      new Pair<>(new Pair<>("t1", 0), new ClientContext("br0", "0.0.0.0", 1234, null, null, now)),//
		      new Pair<>(new Pair<>("t1", 1), new ClientContext("br1", "0.0.0.1", 1234, null, null, now)),//
		      new Pair<>(new Pair<>("t3", 0), new ClientContext("br0", "0.0.0.0", 1234, null, null, now)),//
		      new Pair<>(new Pair<>("t3", 1), new ClientContext("br1", "0.0.0.1", 1234, null, null, now))//

		      ));
		Map<String, Assignment<Integer>> assignments = m_strategy.assign(brokers, topics, originAssignments);

		assertAssignment(assignments,//
		      topics,//
		      Arrays.asList(//
		            new ClientContext("br0", "0.0.0.0", 1234, null, null, now),//
		            new ClientContext("br1", "0.0.0.1", 1234, null, null, now)//
		      ));
	}

	@Test
	public void testTopicAndBrokerAllChanged() throws Exception {
		long now = System.currentTimeMillis();
		Map<String, ClientContext> brokers = createBrokers(Arrays.asList(//
		      new ClientContext("br0", "0.0.0.0", 1234, null, null, now),//
		      new ClientContext("br2", "0.0.0.2", 1234, null, null, now)//
		      ));

		List<Topic> topics = new ArrayList<>();
		topics.add(createTopic("t1", 2, Endpoint.BROKER));
		topics.add(createTopic("t2", 2, Endpoint.KAFKA));
		topics.add(createTopic("t4", 2, Endpoint.BROKER));

		Map<String, Assignment<Integer>> originAssignments = createAssignments(Arrays.asList(//
		      new Pair<>(new Pair<>("t1", 0), new ClientContext("br0", "0.0.0.0", 1234, null, null, now)),//
		      new Pair<>(new Pair<>("t1", 1), new ClientContext("br1", "0.0.0.1", 1234, null, null, now)),//
		      new Pair<>(new Pair<>("t3", 0), new ClientContext("br0", "0.0.0.0", 1234, null, null, now)),//
		      new Pair<>(new Pair<>("t3", 1), new ClientContext("br1", "0.0.0.1", 1234, null, null, now))//

		      ));
		Map<String, Assignment<Integer>> assignments = m_strategy.assign(brokers, topics, originAssignments);

		assertAssignment(assignments,//
		      topics,//
		      Arrays.asList(//
		            new ClientContext("br0", "0.0.0.0", 1234, null, null, now),//
		            new ClientContext("br2", "0.0.0.2", 1234, null, null, now)//
		      ));
	}

	private void assertAssignment(Map<String, Assignment<Integer>> assignments, List<Topic> topics,
	      List<ClientContext> brokers) {
		Map<Pair<String, Integer>, String> tp2Broker = new HashMap<>();
		Map<String, Integer> broker2PartitionCount = new HashMap<>();
		int topicCount = 0;
		int tpCount = 0;
		for (Topic topic : topics) {
			if (Endpoint.BROKER.equals(topic.getEndpointType())) {
				topicCount++;
				for (Partition partition : topic.getPartitions()) {
					Pair<String, Integer> tp = new Pair<>(topic.getName(), partition.getId());
					assertFalse(tp2Broker.containsKey(tp));
					Assignment<Integer> topicAssignment = assignments.get(tp.getKey());
					assertEquals(1, topicAssignment.getAssignment(partition.getId()).size());
					ClientContext broker = topicAssignment.getAssignment(partition.getId()).values().iterator().next();
					assertBrokerContains(brokers, broker);
					tp2Broker.put(tp, broker.getName());
					tpCount++;
					String brokerName = tp2Broker.get(tp);
					if (!broker2PartitionCount.containsKey(brokerName)) {
						broker2PartitionCount.put(brokerName, 0);
					}
					broker2PartitionCount.put(brokerName, broker2PartitionCount.get(brokerName) + 1);
				}
			}
		}

		assertEquals(topicCount, assignments.size());

		int avgPartitionCountPerBroker = tpCount / brokers.size();
		for (Integer partitionCount : broker2PartitionCount.values()) {
			assertTrue(partitionCount == avgPartitionCountPerBroker || partitionCount == avgPartitionCountPerBroker + 1
			      || partitionCount == avgPartitionCountPerBroker - 1);
		}
	}

	private void assertBrokerContains(List<ClientContext> brokers, ClientContext broker) {
		for (ClientContext clientContext : brokers) {
			if (clientContext.getName().equals(broker.getName())) {
				assertEquals(clientContext.getIp(), broker.getIp());
				assertEquals(clientContext.getPort(), broker.getPort());
				return;
			}
		}

		fail();
	}

	private Map<String, Assignment<Integer>> createAssignments(
	      List<Pair<Pair<String, Integer>, ClientContext>> assignments) {
		Map<String, Assignment<Integer>> res = new HashMap<>();

		if (assignments != null && !assignments.isEmpty()) {
			for (Pair<Pair<String, Integer>, ClientContext> pair : assignments) {
				Pair<String, Integer> tp = pair.getKey();
				ClientContext cc = pair.getValue();

				String topic = tp.getKey();
				if (!res.containsKey(topic)) {
					res.put(topic, new Assignment<Integer>());
				}

				Map<String, ClientContext> clients = new HashMap<>();
				clients.put(cc.getName(), cc);
				res.get(topic).addAssignment(tp.getValue(), clients);

			}
		}

		return res;
	}

	private Topic createTopic(String topicName, int partitionCount, String endpointType) {
		Topic t = new Topic(topicName);
		t.setPartitionCount(partitionCount);
		t.setEndpointType(endpointType);

		for (int i = 0; i < partitionCount; i++) {
			Partition partition = new Partition(i);
			t.addPartition(partition);
		}

		return t;
	}

	private Map<String, ClientContext> createBrokers(List<ClientContext> brokerContexts) {
		Map<String, ClientContext> brokers = new LinkedHashMap<>();
		if (brokerContexts != null && !brokerContexts.isEmpty()) {
			for (ClientContext context : brokerContexts) {
				brokers.put(context.getName(), context);
			}
		}
		return brokers;
	}
}
