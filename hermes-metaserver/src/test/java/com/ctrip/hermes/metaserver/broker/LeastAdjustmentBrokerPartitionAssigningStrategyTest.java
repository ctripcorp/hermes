package com.ctrip.hermes.metaserver.broker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.junit.Test;
import org.unidal.helper.Reflects;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaserver.assign.AssignBalancer;
import com.ctrip.hermes.metaserver.assign.LeastAdjustmentAssianBalancer;
import com.ctrip.hermes.metaserver.commons.Assignment;
import com.ctrip.hermes.metaserver.commons.ClientContext;

public class LeastAdjustmentBrokerPartitionAssigningStrategyTest {

	private Map<String, ClientContext> generateBrokers(int size, String prefix) {
		Map<String, ClientContext> brokers = new HashMap<>();
		for (int i = 1; i <= size; i++) {
			brokers.put(prefix + i, new ClientContext(prefix + i, "a", 1, "a", "a", 1L));
		}
		return brokers;
	}

	private Map<String, Topic> generateTopicsWithIncrementalPartitionCount(int size, String prefix) {
		Map<String, Topic> topicMap = new HashMap<>();
		for (int i = 1; i <= size; i++) {
			topicMap.put(prefix + i, new Topic(prefix + i));
			Topic t = topicMap.get("t" + i);
			for (int j = 0; j < i; j++) {
				t.addPartition(new Partition(j));
			}
		}
		return topicMap;
	}

	private void printResult(Map<String, Assignment<Integer>> originAssignments,
	      Map<String, Assignment<Integer>> newAssignments) {
		System.out.println("---------------------------------------------------------------");
		System.out.println("Origin Assignments:");
		for (Entry<String, Assignment<Integer>> assign : originAssignments.entrySet()) {
			System.out.println(assign.getKey());
			for (Entry<Integer, Map<String, ClientContext>> partitionAssign : assign.getValue().getAssignments()
			      .entrySet()) {
				System.out.println(partitionAssign);
			}
		}
		System.out.println("---------------------------------------------------------------");
		System.out.println("New Assignments:");
		for (Entry<String, Assignment<Integer>> assign : newAssignments.entrySet()) {
			System.out.println(assign.getKey());
			for (Entry<Integer, Map<String, ClientContext>> partitionAssign : assign.getValue().getAssignments()
			      .entrySet()) {
				System.out.println(partitionAssign);
			}
		}
	}

	private void normalAssert(Map<String, ClientContext> brokers, List<Topic> topics,
	      Map<String, Assignment<Integer>> newAssignments) {
		Map<String, Set<Pair<String, Integer>>> mapBrokerToTopics = new HashMap<>();
		for (Entry<String, Assignment<Integer>> topicAssign : newAssignments.entrySet()) {
			for (Entry<Integer, Map<String, ClientContext>> tpAssign : topicAssign.getValue().getAssignments().entrySet()) {
				ClientContext broker = tpAssign.getValue().entrySet().iterator().next().getValue();
				if (!mapBrokerToTopics.containsKey(broker.getName())) {
					mapBrokerToTopics.put(broker.getName(), new HashSet<Pair<String, Integer>>());
				}

				Set<Pair<String, Integer>> tpSet = mapBrokerToTopics.get(broker.getName());
				Pair<String, Integer> tp = new Pair<String, Integer>(topicAssign.getKey(), tpAssign.getKey());
				if (tpSet.contains(tp)) {
					throw new RuntimeException(String.format("TP: %s has more than one assinment!", tp));
				}
				tpSet.add(tp);
			}
		}
		assertEquals(brokers.size(), mapBrokerToTopics.size());
		assertEquals(topics.size(), newAssignments.size());

		int partitionCount = 0;
		for (Topic topic : topics) {
			partitionCount += topic.getPartitions().size();
		}
		int avg = partitionCount / brokers.size();
		for (Entry<String, Set<Pair<String, Integer>>> brokerAssignment : mapBrokerToTopics.entrySet()) {
			assertTrue(brokerAssignment.getValue().size() >= avg);
			assertTrue(brokerAssignment.getValue().size() <= avg + 1);
		}
	}

	@Test
	public void AddAndDeleteBrokerTest() {
		LeastAdjustmentBrokerPartitionAssigningStrategy strategy = new LeastAdjustmentBrokerPartitionAssigningStrategy();
		AssignBalancer assignBalancer = new LeastAdjustmentAssianBalancer();
		Reflects.forField().setDeclaredFieldValue(LeastAdjustmentBrokerPartitionAssigningStrategy.class,
		      "m_assignBalancer", strategy, assignBalancer);

		Map<String, ClientContext> brokers = generateBrokers(5, "b");
		Map<String, Topic> topicMap = generateTopicsWithIncrementalPartitionCount(10, "t");
		List<Topic> topics = new ArrayList<>(topicMap.values());

		Map<String, Assignment<Integer>> originAssignments = new HashMap<>();

		// 10 topic Assignments
		int brokerPos = 0;
		int brokerListSize = brokers.size();
		for (int i = 1; i <= 10; i++) {
			originAssignments.put("t" + i, new Assignment<Integer>());
			Assignment<Integer> assignment = originAssignments.get("t" + i);
			for (int j = 0; j < i; j++) {
				Map<String, ClientContext> brokerAssign = new HashMap<>();
				brokerAssign.put("b" + (brokerPos + 1), brokers.get("b" + (brokerPos + 1)));
				brokerPos = (brokerPos + 1) % brokerListSize;
				assignment.addAssignment(j, brokerAssign);
			}
		}

		brokers.remove("b1");
		brokers.put("b6", new ClientContext("b6", "a", 1, "a", "a", 1L));
		brokers.put("b7", new ClientContext("b7", "a", 1, "a", "a", 1L));

		Map<String, Assignment<Integer>> newAssignments = strategy.assign(brokers, topics, originAssignments);

		System.out.println("AddAndDeleteBrokerTest:");
		System.out.println("---------------------------------------------------------------");
		System.out.println("Current Brokers:");
		for (String broker : brokers.keySet()) {
			System.out.print(broker + " ");
		}
		System.out.println();
		printResult(originAssignments, newAssignments);
		normalAssert(brokers, topics, newAssignments);

		Set<ClientContext> inuseBrokers = new HashSet<>();
		for (Entry<String, Assignment<Integer>> assign : newAssignments.entrySet()) {
			Map<Integer, Map<String, ClientContext>> pAssign = assign.getValue().getAssignments();
			for (Map<String, ClientContext> broker : pAssign.values()) {
				inuseBrokers.addAll(broker.values());
			}
		}
		assertFalse(inuseBrokers.contains(brokers.get("b1")));
		assertTrue(inuseBrokers.contains(brokers.get("b6")));
		assertTrue(inuseBrokers.contains(brokers.get("b7")));
	}

	@Test
	public void AddAndDeleteTPTest() {
		LeastAdjustmentBrokerPartitionAssigningStrategy strategy = new LeastAdjustmentBrokerPartitionAssigningStrategy();
		AssignBalancer assignBalancer = new LeastAdjustmentAssianBalancer();
		Reflects.forField().setDeclaredFieldValue(LeastAdjustmentBrokerPartitionAssigningStrategy.class,
		      "m_assignBalancer", strategy, assignBalancer);

		Map<String, ClientContext> brokers = generateBrokers(5, "b");
		Map<String, Assignment<Integer>> originAssignments = new HashMap<>();

		Map<String, Topic> topicMap = generateTopicsWithIncrementalPartitionCount(10, "t");
		List<Topic> topics = new ArrayList<>(topicMap.values());

		// 10 topic Assignments
		int brokerPos = 0;
		int brokerListSize = brokers.size();
		for (int i = 1; i <= 10; i++) {
			originAssignments.put("t" + i, new Assignment<Integer>());
			Assignment<Integer> assignment = originAssignments.get("t" + i);
			for (int j = 0; j < i; j++) {
				Map<String, ClientContext> brokerAssign = new HashMap<>();
				brokerAssign.put("b" + (brokerPos + 1), brokers.get("b" + (brokerPos + 1)));
				brokerPos = (brokerPos + 1) % brokerListSize;
				assignment.addAssignment(j, brokerAssign);
			}
		}

		topics.remove(new Topic("t1"));
		Topic t = new Topic("t11");
		for (int i = 0; i < 10; i++) {
			t.addPartition(new Partition(i));
		}
		topics.add(t);
		t = new Topic("t12");
		for (int i = 0; i < 10; i++) {
			t.addPartition(new Partition(i));
		}
		topics.add(t);

		Map<String, Assignment<Integer>> newAssignments = strategy.assign(brokers, topics, originAssignments);

		System.out.println("AddAndDeleteBrokerTest:");
		System.out.println("---------------------------------------------------------------");
		System.out.println("Current Topics:");
		for (Topic topic : topics) {
			System.out.print(topic.getName() + " ");
		}
		System.out.println();
		printResult(originAssignments, newAssignments);
		normalAssert(brokers, topics, newAssignments);
		assertFalse(newAssignments.keySet().contains("t1"));
		assertTrue(newAssignments.keySet().contains("t11"));
		assertTrue(newAssignments.keySet().contains("t12"));
	}

	@Test
	public void AddAndDeleteBrokerAndTPTest() {
		LeastAdjustmentBrokerPartitionAssigningStrategy strategy = new LeastAdjustmentBrokerPartitionAssigningStrategy();
		AssignBalancer assignBalancer = new LeastAdjustmentAssianBalancer();
		Reflects.forField().setDeclaredFieldValue(LeastAdjustmentBrokerPartitionAssigningStrategy.class,
		      "m_assignBalancer", strategy, assignBalancer);

		Map<String, ClientContext> brokers = generateBrokers(5, "b");
		Map<String, Assignment<Integer>> originAssignments = new HashMap<>();

		Map<String, Topic> topicMap = generateTopicsWithIncrementalPartitionCount(10, "t");
		List<Topic> topics = new ArrayList<>(topicMap.values());

		// 10 topic Assignments
		int brokerPos = 0;
		int brokerListSize = brokers.size();
		for (int i = 1; i <= 10; i++) {
			originAssignments.put("t" + i, new Assignment<Integer>());
			Assignment<Integer> assignment = originAssignments.get("t" + i);
			for (int j = 0; j < i; j++) {
				Map<String, ClientContext> brokerAssign = new HashMap<>();
				brokerAssign.put("b" + (brokerPos + 1), brokers.get("b" + (brokerPos + 1)));
				brokerPos = (brokerPos + 1) % brokerListSize;
				assignment.addAssignment(j, brokerAssign);
			}
		}

		brokers.remove("b1");
		brokers.put("b7", new ClientContext("b7", "a", 1, "a", "a", 1L));

		topics.remove(new Topic("t10"));
		Topic t = new Topic("t11");
		for (int i = 0; i < 10; i++) {
			t.addPartition(new Partition(i));
		}
		topics.add(t);

		Map<String, Assignment<Integer>> newAssignments = strategy.assign(brokers, topics, originAssignments);

		System.out.println("AddAndDeleteBrokerTest:");
		System.out.println("---------------------------------------------------------------");
		System.out.println("Current Brokers:");
		for (String broker : brokers.keySet()) {
			System.out.print(broker + " ");
		}
		System.out.println();
		System.out.println("---------------------------------------------------------------");
		System.out.println("Current Topics:");
		for (Topic topic : topics) {
			System.out.print(topic.getName() + " ");
		}
		System.out.println();
		printResult(originAssignments, newAssignments);
		normalAssert(brokers, topics, newAssignments);
		assertFalse(newAssignments.keySet().contains("t10"));
		assertTrue(newAssignments.keySet().contains("t11"));

		Set<ClientContext> inuseBrokers = new HashSet<>();
		for (Entry<String, Assignment<Integer>> assign : newAssignments.entrySet()) {
			Map<Integer, Map<String, ClientContext>> pAssign = assign.getValue().getAssignments();
			for (Map<String, ClientContext> broker : pAssign.values()) {
				inuseBrokers.addAll(broker.values());
			}
		}
		assertFalse(inuseBrokers.contains(brokers.get("b1")));
		assertTrue(inuseBrokers.contains(brokers.get("b7")));
	}

	@Test
	public void AddAndDeletPartitionTest() {
		LeastAdjustmentBrokerPartitionAssigningStrategy strategy = new LeastAdjustmentBrokerPartitionAssigningStrategy();
		AssignBalancer assignBalancer = new LeastAdjustmentAssianBalancer();
		Reflects.forField().setDeclaredFieldValue(LeastAdjustmentBrokerPartitionAssigningStrategy.class,
		      "m_assignBalancer", strategy, assignBalancer);

		Map<String, ClientContext> brokers = generateBrokers(5, "b");
		Map<String, Assignment<Integer>> originAssignments = new HashMap<>();

		Map<String, Topic> topicMap = generateTopicsWithIncrementalPartitionCount(10, "t");
		List<Topic> topics = new ArrayList<>(topicMap.values());

		// 10 topic Assignments
		int brokerPos = 0;
		int brokerListSize = brokers.size();
		for (int i = 1; i <= 10; i++) {
			originAssignments.put("t" + i, new Assignment<Integer>());
			Assignment<Integer> assignment = originAssignments.get("t" + i);
			for (int j = 0; j < i; j++) {
				Map<String, ClientContext> brokerAssign = new HashMap<>();
				brokerAssign.put("b" + (brokerPos + 1), brokers.get("b" + (brokerPos + 1)));
				brokerPos = (brokerPos + 1) % brokerListSize;
				assignment.addAssignment(j, brokerAssign);
			}
		}

		Topic t9 = topicMap.get("t9");
		t9.removePartition(5);
		Topic t10 = topicMap.get("t10");
		t10.addPartition(new Partition(15));

		Map<String, Assignment<Integer>> newAssignments = strategy.assign(brokers, topics, originAssignments);

		System.out.println("AddAndDeleteBrokerTest:");
		System.out.println("---------------------------------------------------------------");
		System.out.println("Current Brokers:");
		for (String broker : brokers.keySet()) {
			System.out.print(broker + " ");
		}
		System.out.println();
		System.out.println("---------------------------------------------------------------");
		System.out.println("Current Topics:");
		for (Topic topic : topics) {
			System.out.print(topic.getName() + " ");
		}
		System.out.println();
		printResult(originAssignments, newAssignments);
		normalAssert(brokers, topics, newAssignments);
		assertTrue(newAssignments.get("t9").getAssignment(5) == null);
		assertTrue(newAssignments.get("t10").getAssignment(15) != null);
	}

}
