package com.ctrip.hermes.metaserver.consumer;

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

import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.metaserver.assign.AssignBalancer;
import com.ctrip.hermes.metaserver.assign.LeastAdjustmentAssianBalancer;
import com.ctrip.hermes.metaserver.commons.ClientContext;

public class LeastAdjustmentConsumerPartitionAssigningStrategyTest {

	private void normalAssert(List<Partition> partitions, Map<String, ClientContext> consumers,
	      Map<Integer, Map<String, ClientContext>> newAssigns) {
		assertTrue(partitions.size() == newAssigns.size());

		Map<Integer, Partition> partitionMap = new HashMap<>();
		for (Partition partition : partitions) {
			partitionMap.put(partition.getId(), partition);
		}

		Map<String, Set<Partition>> mapConsumerToPartition = new HashMap<>();
		for (Entry<Integer, Map<String, ClientContext>> assign : newAssigns.entrySet()) {
			String broker = assign.getValue().entrySet().iterator().next().getKey();
			if (!mapConsumerToPartition.containsKey(broker)) {
				mapConsumerToPartition.put(broker, new HashSet<Partition>());
			}
			Partition partition = partitionMap.get(assign.getKey());
			assertFalse(mapConsumerToPartition.get(broker).contains(partition));
			mapConsumerToPartition.get(broker).add(partition);
		}

		assertTrue(mapConsumerToPartition.size() == consumers.size());

		int avg = partitions.size() / consumers.size();
		for (Entry<String, Set<Partition>> assign : mapConsumerToPartition.entrySet()) {
			int size = assign.getValue().size();
			assertTrue(size >= avg);
			assertTrue(size <= avg + 1);
		}
	}

	@Test
	public void testPartitionAddAndDelete() {
		LeastAdjustmentConsumerPartitionAssigningStrategy strategy = new LeastAdjustmentConsumerPartitionAssigningStrategy();
		AssignBalancer assignBalancer = new LeastAdjustmentAssianBalancer();
		Reflects.forField().setDeclaredFieldValue(LeastAdjustmentConsumerPartitionAssigningStrategy.class,
		      "m_assignBalancer", strategy, assignBalancer);

		List<Partition> partitions = new ArrayList<>();
		for (int i = 1; i < 10; i++) {
			Partition p = new Partition(i);
			partitions.add(p);
		}

		Map<String, ClientContext> currentConsumers = new HashMap<>();
		final ClientContext a = new ClientContext("a", "a", 1, "a", "a", 1L);
		final ClientContext b = new ClientContext("b", "a", 1, "a", "a", 1L);
		final ClientContext c = new ClientContext("c", "a", 1, "a", "a", 1L);
		final ClientContext d = new ClientContext("d", "a", 1, "a", "a", 1L);
		final ClientContext e = new ClientContext("e", "a", 1, "a", "a", 1L);
		currentConsumers.put("a", a);
		currentConsumers.put("b", b);
		currentConsumers.put("c", c);
		currentConsumers.put("d", d);
		currentConsumers.put("e", e);
		Map<Integer, Map<String, ClientContext>> originAssigns = new HashMap<>();
		originAssigns.put(0, new HashMap<String, ClientContext>() {
			{
				put("a", a);
			}
		});
		originAssigns.put(1, new HashMap<String, ClientContext>() {
			{
				put("b", b);
			}
		});
		originAssigns.put(2, new HashMap<String, ClientContext>() {
			{
				put("c", c);
			}
		});
		originAssigns.put(3, new HashMap<String, ClientContext>() {
			{
				put("d", d);
			}
		});
		originAssigns.put(4, new HashMap<String, ClientContext>() {
			{
				put("e", e);
			}
		});
		originAssigns.put(5, new HashMap<String, ClientContext>() {
			{
				put("e", a);
			}
		});

		Map<Integer, Map<String, ClientContext>> newAssigns = strategy
		      .assign(partitions, currentConsumers, originAssigns);
		System.out.println("TestPartitionAddAndDelete:");
		System.out.println("---------------------------------------------------------------");
		System.out.println("Current Partitions:");
		for (Partition partition : partitions) {
			System.out.print(partition.getId() + " ");
		}
		System.out.println();
		System.out.println("---------------------------------------------------------------");
		System.out.println("Origin Assignments:");
		for (Entry<Integer, Map<String, ClientContext>> assign : originAssigns.entrySet()) {
			System.out.println(assign);
		}
		System.out.println("---------------------------------------------------------------");
		System.out.println("New Assignments:");
		for (Entry<Integer, Map<String, ClientContext>> assign : newAssigns.entrySet()) {
			System.out.println(assign);
		}

		normalAssert(partitions, currentConsumers, newAssigns);

	}

	@Test
	public void testConsumerAddAndDelete() {
		LeastAdjustmentConsumerPartitionAssigningStrategy strategy = new LeastAdjustmentConsumerPartitionAssigningStrategy();
		AssignBalancer assignBalancer = new LeastAdjustmentAssianBalancer();
		Reflects.forField().setDeclaredFieldValue(LeastAdjustmentConsumerPartitionAssigningStrategy.class,
		      "m_assignBalancer", strategy, assignBalancer);

		List<Partition> partitions = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			Partition p = new Partition(i);
			partitions.add(p);
		}

		Map<String, ClientContext> currentConsumers = new HashMap<>();
		final ClientContext a = new ClientContext("a", "a", 1, "a", "a", 1L);
		final ClientContext b = new ClientContext("b", "a", 1, "a", "a", 1L);
		final ClientContext c = new ClientContext("c", "a", 1, "a", "a", 1L);
		final ClientContext d = new ClientContext("d", "a", 1, "a", "a", 1L);
		final ClientContext e = new ClientContext("e", "a", 1, "a", "a", 1L);
		final ClientContext f = new ClientContext("f", "a", 1, "a", "a", 1L);
		currentConsumers.put("a", a);
		currentConsumers.put("b", b);
		currentConsumers.put("c", c);
		currentConsumers.put("f", f);
		Map<Integer, Map<String, ClientContext>> originAssigns = new HashMap<>();
		originAssigns.put(0, new HashMap<String, ClientContext>() {
			{
				put("a", a);
			}
		});
		originAssigns.put(1, new HashMap<String, ClientContext>() {
			{
				put("b", b);
			}
		});
		originAssigns.put(2, new HashMap<String, ClientContext>() {
			{
				put("c", c);
			}
		});
		originAssigns.put(3, new HashMap<String, ClientContext>() {
			{
				put("d", d);
			}
		});
		originAssigns.put(4, new HashMap<String, ClientContext>() {
			{
				put("e", e);
			}
		});
		originAssigns.put(5, new HashMap<String, ClientContext>() {
			{
				put("a", a);
			}
		});

		Map<Integer, Map<String, ClientContext>> newAssigns = strategy
		      .assign(partitions, currentConsumers, originAssigns);
		System.out.println("TestConsumerAddAndDelete:");
		System.out.println("---------------------------------------------------------------");
		System.out.println("Current Consumers:");
		for (Entry<String, ClientContext> consumer : currentConsumers.entrySet()) {
			System.out.print(consumer.getKey() + " ");
		}
		System.out.println();
		System.out.println("---------------------------------------------------------------");
		System.out.println("Origin Assignments:");
		for (Entry<Integer, Map<String, ClientContext>> assign : originAssigns.entrySet()) {
			System.out.println(assign);
		}
		System.out.println("---------------------------------------------------------------");
		System.out.println("New Assignments:");
		for (Entry<Integer, Map<String, ClientContext>> assign : newAssigns.entrySet()) {
			System.out.println(assign);
		}

		normalAssert(partitions, currentConsumers, newAssigns);

	}

	@Test
	public void testParitionAndConsumerAddAndDelete() {
		LeastAdjustmentConsumerPartitionAssigningStrategy strategy = new LeastAdjustmentConsumerPartitionAssigningStrategy();
		AssignBalancer assignBalancer = new LeastAdjustmentAssianBalancer();
		Reflects.forField().setDeclaredFieldValue(LeastAdjustmentConsumerPartitionAssigningStrategy.class,
		      "m_assignBalancer", strategy, assignBalancer);

		List<Partition> partitions = new ArrayList<>();
		for (int i = 0; i < 3; i++) {
			Partition p = new Partition(i);
			partitions.add(p);
		}
		partitions.add(new Partition(5));
		partitions.add(new Partition(10));
		partitions.add(new Partition(11));
		partitions.add(new Partition(12));
		Map<String, ClientContext> currentConsumers = new HashMap<>();
		final ClientContext a = new ClientContext("a", "a", 1, "a", "a", 1L);
		final ClientContext b = new ClientContext("b", "a", 1, "a", "a", 1L);
		final ClientContext c = new ClientContext("c", "a", 1, "a", "a", 1L);
		final ClientContext d = new ClientContext("d", "a", 1, "a", "a", 1L);
		final ClientContext e = new ClientContext("e", "a", 1, "a", "a", 1L);
		final ClientContext f = new ClientContext("f", "a", 1, "a", "a", 1L);
		currentConsumers.put("a", a);
		currentConsumers.put("b", b);
		currentConsumers.put("c", c);
		currentConsumers.put("f", f);
		Map<Integer, Map<String, ClientContext>> originAssigns = new HashMap<>();
		originAssigns.put(0, new HashMap<String, ClientContext>() {
			{
				put("a", a);
			}
		});
		originAssigns.put(1, new HashMap<String, ClientContext>() {
			{
				put("b", b);
			}
		});
		originAssigns.put(2, new HashMap<String, ClientContext>() {
			{
				put("c", c);
			}
		});
		originAssigns.put(3, new HashMap<String, ClientContext>() {
			{
				put("d", d);
			}
		});
		originAssigns.put(4, new HashMap<String, ClientContext>() {
			{
				put("e", e);
			}
		});
		originAssigns.put(5, new HashMap<String, ClientContext>() {
			{
				put("a", a);
			}
		});

		Map<Integer, Map<String, ClientContext>> newAssigns = strategy
		      .assign(partitions, currentConsumers, originAssigns);
		System.out.println("TestConsumerAddAndDelete:");
		System.out.println("---------------------------------------------------------------");
		System.out.println("Current Consumers:");
		for (Entry<String, ClientContext> consumer : currentConsumers.entrySet()) {
			System.out.print(consumer.getKey() + " ");
		}
		System.out.println();
		System.out.println("---------------------------------------------------------------");
		System.out.println("Current Partitions:");
		for (Partition partition : partitions) {
			System.out.print(partition.getId() + " ");
		}
		System.out.println();
		System.out.println("---------------------------------------------------------------");
		System.out.println("Origin Assignments:");
		for (Entry<Integer, Map<String, ClientContext>> assign : originAssigns.entrySet()) {
			System.out.println(assign);
		}
		System.out.println("---------------------------------------------------------------");
		System.out.println("New Assignments:");
		for (Entry<Integer, Map<String, ClientContext>> assign : newAssigns.entrySet()) {
			System.out.println(assign);
		}

		normalAssert(partitions, currentConsumers, newAssigns);
	}

}
