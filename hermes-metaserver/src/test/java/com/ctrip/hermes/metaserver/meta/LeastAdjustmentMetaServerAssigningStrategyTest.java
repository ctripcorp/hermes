package com.ctrip.hermes.metaserver.meta;

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

import com.ctrip.hermes.meta.entity.Server;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaserver.assign.AssignBalancer;
import com.ctrip.hermes.metaserver.assign.LeastAdjustmentAssianBalancer;
import com.ctrip.hermes.metaserver.commons.Assignment;
import com.ctrip.hermes.metaserver.commons.ClientContext;

public class LeastAdjustmentMetaServerAssigningStrategyTest {

	private void normalAssert(List<Server> metaServers, List<Topic> topics, Assignment<String> newAssigns) {
		assertTrue(newAssigns.getAssignments().size() == topics.size());
		Map<String, Set<String>> mapMetaServerToTps = new HashMap<>();

		for (Entry<String, Map<String, ClientContext>> tAssign : newAssigns.getAssignments().entrySet()) {
			String topic = tAssign.getKey();
			assertTrue(tAssign.getValue().size() == 1);
			Entry<String, ClientContext> metaServer = tAssign.getValue().entrySet().iterator().next();
			if (!mapMetaServerToTps.containsKey(metaServer.getKey())) {
				mapMetaServerToTps.put(metaServer.getKey(), new HashSet<String>());
			}
			assertFalse(mapMetaServerToTps.get(metaServer.getKey()).contains(topic));
			mapMetaServerToTps.get(metaServer.getKey()).add(topic);
		}

		assertTrue(mapMetaServerToTps.size() == metaServers.size());

		int avg = topics.size() / metaServers.size();
		for (Set<String> topicSet : mapMetaServerToTps.values()) {
			assertTrue(topicSet.size() >= avg);
			assertTrue(topicSet.size() <= avg + 1);
		}
	}

	@Test
	public void AddAndDeleteMetaServerTest() {
		LeastAdjustmentMetaServerAssigningStrategy strategy = new LeastAdjustmentMetaServerAssigningStrategy();
		AssignBalancer assignBalancer = new LeastAdjustmentAssianBalancer();
		Reflects.forField().setDeclaredFieldValue(LeastAdjustmentMetaServerAssigningStrategy.class, "m_assignBalancer",
		      strategy, assignBalancer);

		List<Server> metaServers = new ArrayList<>();
		metaServers.add(new Server("a").setPort(1));
		// metaServers.add(new Server("b").setPort(1));
		metaServers.add(new Server("c").setPort(1));
		metaServers.add(new Server("d").setPort(1));
		metaServers.add(new Server("e").setPort(1));

		List<Topic> topics = new ArrayList<>();
		for (int i = 0; i < 6; i++) {
			topics.add(new Topic(String.valueOf(i)));
		}

		Assignment<String> originAssignments = new Assignment<>();
		final ClientContext a = new ClientContext("a", "a", 1, "a", "a", 1L);
		final ClientContext b = new ClientContext("b", "a", 1, "a", "a", 1L);
		final ClientContext c = new ClientContext("c", "a", 1, "a", "a", 1L);

		originAssignments.addAssignment("0", new HashMap<String, ClientContext>() {
			{
				put("a", a);
			}
		});
		originAssignments.addAssignment("1", new HashMap<String, ClientContext>() {
			{
				put("b", b);
			}
		});
		originAssignments.addAssignment("2", new HashMap<String, ClientContext>() {
			{
				put("c", c);
			}
		});
		originAssignments.addAssignment("3", new HashMap<String, ClientContext>() {
			{
				put("a", a);
			}
		});
		originAssignments.addAssignment("4", new HashMap<String, ClientContext>() {
			{
				put("a", a);
			}
		});
		originAssignments.addAssignment("5", new HashMap<String, ClientContext>() {
			{
				put("b", b);
			}
		});

		Assignment<String> newAssigns = strategy.assign(metaServers, topics, originAssignments);
		System.out.println("AddAndDeleteMetaServerTest:");
		System.out.println("---------------------------------------------------------------");
		System.out.println("Current MetaServers:");
		for (Server server : metaServers) {
			System.out.println(server.getId());
		}
		System.out.println();
		System.out.println("---------------------------------------------------------------");
		System.out.println("Origin Assignments:");
		for (Entry<String, Map<String, ClientContext>> assign : originAssignments.getAssignments().entrySet()) {
			System.out.println(assign);
		}
		System.out.println("---------------------------------------------------------------");
		System.out.println("New Assignments:");
		for (Entry<String, Map<String, ClientContext>> assign : newAssigns.getAssignments().entrySet()) {
			System.out.println(assign);
		}

		normalAssert(metaServers, topics, newAssigns);
		Set<String> inuseMetaServers = new HashSet<>();
		for (Entry<String, Map<String, ClientContext>> topicAssign : newAssigns.getAssignments().entrySet()) {
			String metaServer = topicAssign.getValue().entrySet().iterator().next().getKey();
			inuseMetaServers.add(metaServer);
		}

		assertFalse(inuseMetaServers.contains("b"));
		assertTrue(inuseMetaServers.contains("d"));
		assertTrue(inuseMetaServers.contains("e"));

	}

	@Test
	public void AddAndDeleteTopicTest() {
		LeastAdjustmentMetaServerAssigningStrategy strategy = new LeastAdjustmentMetaServerAssigningStrategy();
		AssignBalancer assignBalancer = new LeastAdjustmentAssianBalancer();
		Reflects.forField().setDeclaredFieldValue(LeastAdjustmentMetaServerAssigningStrategy.class, "m_assignBalancer",
		      strategy, assignBalancer);

		List<Server> metaServers = new ArrayList<>();
		metaServers.add(new Server("a").setPort(1));
		metaServers.add(new Server("b").setPort(1));
		metaServers.add(new Server("c").setPort(1));
		// metaServers.add(new Server("d").setPort(1));
		// metaServers.add(new Server("e").setPort(1));

		List<Topic> topics = new ArrayList<>();
		for (int i = 0; i < 4; i++) {
			topics.add(new Topic(String.valueOf(i)));
		}

		topics.add(new Topic("10"));
		topics.add(new Topic("11"));
		topics.add(new Topic("12"));

		Assignment<String> originAssignments = new Assignment<>();
		final ClientContext a = new ClientContext("a", "a", 1, "a", "a", 1L);
		final ClientContext b = new ClientContext("b", "a", 1, "a", "a", 1L);
		final ClientContext c = new ClientContext("c", "a", 1, "a", "a", 1L);

		originAssignments.addAssignment("0", new HashMap<String, ClientContext>() {
			{
				put("a", a);
			}
		});
		originAssignments.addAssignment("1", new HashMap<String, ClientContext>() {
			{
				put("b", b);
			}
		});
		originAssignments.addAssignment("2", new HashMap<String, ClientContext>() {
			{
				put("c", c);
			}
		});
		originAssignments.addAssignment("3", new HashMap<String, ClientContext>() {
			{
				put("a", a);
			}
		});
		originAssignments.addAssignment("4", new HashMap<String, ClientContext>() {
			{
				put("a", a);
			}
		});
		originAssignments.addAssignment("5", new HashMap<String, ClientContext>() {
			{
				put("b", b);
			}
		});

		Assignment<String> newAssigns = strategy.assign(metaServers, topics, originAssignments);
		System.out.println("AddAndDeleteTopicTest:");
		System.out.println("---------------------------------------------------------------");
		System.out.println("Current Topics:");
		for (Topic topic : topics) {
			System.out.println(topic.getName());
		}
		System.out.println();
		System.out.println("---------------------------------------------------------------");
		System.out.println("Origin Assignments:");
		for (Entry<String, Map<String, ClientContext>> assign : originAssignments.getAssignments().entrySet()) {
			System.out.println(assign);
		}
		System.out.println("---------------------------------------------------------------");
		System.out.println("New Assignments:");
		for (Entry<String, Map<String, ClientContext>> assign : newAssigns.getAssignments().entrySet()) {
			System.out.println(assign);
		}

		normalAssert(metaServers, topics, newAssigns);
		assertTrue(newAssigns.getAssignment("5") == null);
		assertTrue(newAssigns.getAssignment("10") != null);
		assertTrue(newAssigns.getAssignment("11") != null);
		assertTrue(newAssigns.getAssignment("12") != null);

	}

	@Test
	public void AddAndDeleteTopicAndMetaServerTest() {
		LeastAdjustmentMetaServerAssigningStrategy strategy = new LeastAdjustmentMetaServerAssigningStrategy();
		AssignBalancer assignBalancer = new LeastAdjustmentAssianBalancer();
		Reflects.forField().setDeclaredFieldValue(LeastAdjustmentMetaServerAssigningStrategy.class, "m_assignBalancer",
		      strategy, assignBalancer);

		List<Server> metaServers = new ArrayList<>();
		metaServers.add(new Server("a").setPort(1));
		// metaServers.add(new Server("b").setPort(1));
		metaServers.add(new Server("c").setPort(1));
		metaServers.add(new Server("d").setPort(1));
		metaServers.add(new Server("e").setPort(1));

		List<Topic> topics = new ArrayList<>();
		for (int i = 0; i < 4; i++) {
			topics.add(new Topic(String.valueOf(i)));
		}

		topics.add(new Topic("10"));
		topics.add(new Topic("11"));
		topics.add(new Topic("12"));

		Assignment<String> originAssignments = new Assignment<>();
		final ClientContext a = new ClientContext("a", "a", 1, "a", "a", 1L);
		final ClientContext b = new ClientContext("b", "a", 1, "a", "a", 1L);
		final ClientContext c = new ClientContext("c", "a", 1, "a", "a", 1L);

		originAssignments.addAssignment("0", new HashMap<String, ClientContext>() {
			{
				put("a", a);
			}
		});
		originAssignments.addAssignment("1", new HashMap<String, ClientContext>() {
			{
				put("b", b);
			}
		});
		originAssignments.addAssignment("2", new HashMap<String, ClientContext>() {
			{
				put("c", c);
			}
		});
		originAssignments.addAssignment("3", new HashMap<String, ClientContext>() {
			{
				put("a", a);
			}
		});
		originAssignments.addAssignment("4", new HashMap<String, ClientContext>() {
			{
				put("a", a);
			}
		});
		originAssignments.addAssignment("5", new HashMap<String, ClientContext>() {
			{
				put("b", b);
			}
		});

		Assignment<String> newAssigns = strategy.assign(metaServers, topics, originAssignments);
		System.out.println("AddAndDeleteTopicAndMetaServerTest:");
		System.out.println("---------------------------------------------------------------");
		System.out.println("Current MetaServers:");
		for (Server server : metaServers) {
			System.out.print(server.getId() + " ");
		}
		System.out.println();
		System.out.println("---------------------------------------------------------------");
		System.out.println("Current Topics:");
		for (Topic topic : topics) {
			System.out.print(topic.getName() + " ");
		}
		System.out.println();
		System.out.println("---------------------------------------------------------------");
		System.out.println("Origin Assignments:");
		for (Entry<String, Map<String, ClientContext>> assign : originAssignments.getAssignments().entrySet()) {
			System.out.println(assign);
		}
		System.out.println("---------------------------------------------------------------");
		System.out.println("New Assignments:");
		for (Entry<String, Map<String, ClientContext>> assign : newAssigns.getAssignments().entrySet()) {
			System.out.println(assign);
		}

		normalAssert(metaServers, topics, newAssigns);
		Set<String> inuseMetaServers = new HashSet<>();
		for (Entry<String, Map<String, ClientContext>> topicAssign : newAssigns.getAssignments().entrySet()) {
			String metaServer = topicAssign.getValue().entrySet().iterator().next().getKey();
			inuseMetaServers.add(metaServer);
		}

		assertFalse(inuseMetaServers.contains("b"));
		assertTrue(inuseMetaServers.contains("d"));
		assertTrue(inuseMetaServers.contains("e"));
		
		assertTrue(newAssigns.getAssignment("5") == null);
		assertTrue(newAssigns.getAssignment("10") != null);
		assertTrue(newAssigns.getAssignment("11") != null);
		assertTrue(newAssigns.getAssignment("12") != null);

	}
}
