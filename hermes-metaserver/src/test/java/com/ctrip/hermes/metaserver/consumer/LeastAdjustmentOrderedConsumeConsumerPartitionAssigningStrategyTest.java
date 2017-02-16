package com.ctrip.hermes.metaserver.consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Test;

import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.metaserver.commons.ClientContext;

public class LeastAdjustmentOrderedConsumeConsumerPartitionAssigningStrategyTest {

	private void vefifyAssign(Map<Integer, Map<String, ClientContext>> assigns, String exp) {
		String[] subExps = exp.split(",");

		assertEquals(subExps.length, assigns.size());

		for (String subExp : subExps) {
			String[] partitionConsumer = subExp.split("=>");
			int partition = Integer.parseInt(partitionConsumer[0].trim());
			String consumer = partitionConsumer[1];
			assertTrue(assigns.get(partition).containsKey(consumer));
			assertEquals(consumer, assigns.get(partition).get(consumer).getName());
		}
	}

	@Test
	public void testFirstAssign() {
		LeastAdjustmentConsumerPartitionAssigningStrategy s = new LeastAdjustmentConsumerPartitionAssigningStrategy();

		List<Partition> partitions = Arrays.asList(p(1), p(2), p(3));

		Map<Integer, Map<String, ClientContext>> originAssigns = null;

		Map<String, ClientContext> currentConsumers = new TreeMap<>();
		currentConsumers.put("c1", cc("c1"));

		Map<Integer, Map<String, ClientContext>> newAssigns = s.assign(partitions, currentConsumers, originAssigns);

		vefifyAssign(newAssigns, "1=>c1,2=>c1,3=>c1");
	}

	@Test
	public void testMoreConsumerThanPartition() {
		LeastAdjustmentConsumerPartitionAssigningStrategy s = new LeastAdjustmentConsumerPartitionAssigningStrategy();

		List<Partition> partitions = Arrays.asList(p(1), p(2), p(3));

		Map<Integer, Map<String, ClientContext>> originAssigns = new TreeMap<>();
		originAssigns.put(1, m("c1"));
		originAssigns.put(2, m("c2"));
		originAssigns.put(3, m("c3"));

		Map<String, ClientContext> currentConsumers = new TreeMap<>();
		currentConsumers.put("c1", cc("c1"));
		currentConsumers.put("c2", cc("c2"));
		currentConsumers.put("c3", cc("c3"));
		currentConsumers.put("c4", cc("c4"));

		Map<Integer, Map<String, ClientContext>> newAssigns = s.assign(partitions, currentConsumers, originAssigns);

		vefifyAssign(newAssigns, "1=>c1,2=>c2,3=>c3");
	}

	@Test
	public void testConsumerDown() {
		LeastAdjustmentConsumerPartitionAssigningStrategy s = new LeastAdjustmentConsumerPartitionAssigningStrategy();

		List<Partition> partitions = Arrays.asList(p(1), p(2), p(3));

		Map<Integer, Map<String, ClientContext>> originAssigns = new TreeMap<>();
		originAssigns.put(1, m("c1"));
		originAssigns.put(2, m("c2"));
		originAssigns.put(3, m("c3"));

		Map<String, ClientContext> currentConsumers = new TreeMap<>();
		currentConsumers.put("c1", cc("c1"));
		currentConsumers.put("c2", cc("c2"));

		Map<Integer, Map<String, ClientContext>> newAssigns = s.assign(partitions, currentConsumers, originAssigns);

		vefifyAssign(newAssigns, "1=>c1,2=>c2,3=>c2");
	}

	@Test
	public void testConsumerUp() {
		LeastAdjustmentConsumerPartitionAssigningStrategy s = new LeastAdjustmentConsumerPartitionAssigningStrategy();

		List<Partition> partitions = Arrays.asList(p(1), p(2), p(3));

		Map<Integer, Map<String, ClientContext>> originAssigns = new TreeMap<>();
		originAssigns.put(1, m("c1"));
		originAssigns.put(2, m("c1"));
		originAssigns.put(3, m("c2"));

		Map<String, ClientContext> currentConsumers = new TreeMap<>();
		currentConsumers.put("c1", cc("c1"));
		currentConsumers.put("c2", cc("c2"));
		currentConsumers.put("c3", cc("c3"));

		Map<Integer, Map<String, ClientContext>> newAssigns = s.assign(partitions, currentConsumers, originAssigns);

		vefifyAssign(newAssigns, "1=>c3,2=>c1,3=>c2");
	}

	@Test
	public void testConsumerDownUp1() {
		LeastAdjustmentConsumerPartitionAssigningStrategy s = new LeastAdjustmentConsumerPartitionAssigningStrategy();

		List<Partition> partitions = Arrays.asList(p(1), p(2), p(3));

		Map<Integer, Map<String, ClientContext>> originAssigns = new TreeMap<>();
		originAssigns.put(1, m("c1"));
		originAssigns.put(2, m("c1"));
		originAssigns.put(3, m("c2"));

		Map<String, ClientContext> currentConsumers = new TreeMap<>();
		currentConsumers.put("c2", cc("c2"));
		currentConsumers.put("c3", cc("c3"));

		Map<Integer, Map<String, ClientContext>> newAssigns = s.assign(partitions, currentConsumers, originAssigns);

		vefifyAssign(newAssigns, "1=>c3,2=>c3,3=>c2");
	}

	@Test
	public void testConsumerDownUp2() {
		LeastAdjustmentConsumerPartitionAssigningStrategy s = new LeastAdjustmentConsumerPartitionAssigningStrategy();

		List<Partition> partitions = Arrays.asList(p(1), p(2), p(3));

		Map<Integer, Map<String, ClientContext>> originAssigns = new TreeMap<>();
		originAssigns.put(1, m("c1"));
		originAssigns.put(2, m("c1"));
		originAssigns.put(3, m("c2"));

		Map<String, ClientContext> currentConsumers = new TreeMap<>();
		currentConsumers.put("c2", cc("c2"));
		currentConsumers.put("c3", cc("c3"));

		Map<Integer, Map<String, ClientContext>> newAssigns = s.assign(partitions, currentConsumers, originAssigns);

		vefifyAssign(newAssigns, "1=>c3,2=>c3,3=>c2");
	}

	private Map<String, ClientContext> m(String consumer) {
		Map<String, ClientContext> map = new TreeMap<>();
		map.put(consumer, cc(consumer));
		return map;
	}

	private ClientContext cc(String name) {
		ClientContext cc = new ClientContext();
		cc.setName(name);
		return cc;
	}

	private Partition p(int id) {
		Partition p = new Partition();
		p.setId(id);
		return p;
	}

}
