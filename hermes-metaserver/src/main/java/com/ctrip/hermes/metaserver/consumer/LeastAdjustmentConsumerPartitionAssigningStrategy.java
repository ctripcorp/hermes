package com.ctrip.hermes.metaserver.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.metaserver.assign.AssignBalancer;
import com.ctrip.hermes.metaserver.commons.ClientContext;

/**
 * @author Marsqing
 *
 */
@Named(type = ConsumerPartitionAssigningStrategy.class)
public class LeastAdjustmentConsumerPartitionAssigningStrategy implements ConsumerPartitionAssigningStrategy {

	private final static Logger log = LoggerFactory.getLogger(LeastAdjustmentConsumerPartitionAssigningStrategy.class);

	@Inject
	private AssignBalancer m_assignBalancer;

	@Override
	public Map<Integer, Map<String, ClientContext>> assign(List<Partition> partitions,
	      Map<String, ClientContext> currentConsumers, Map<Integer, Map<String, ClientContext>> originAssigns) {
		Map<Integer, Map<String, ClientContext>> result = new HashMap<>();

		if (partitions == null || partitions.isEmpty() || currentConsumers == null || currentConsumers.isEmpty()) {
			return result;
		}

		if (originAssigns == null) {
			originAssigns = Collections.emptyMap();
		}

		Map<String, List<Integer>> originConsumerToPartition = mapConsumerToPartitions(partitions, currentConsumers,
		      originAssigns);
		List<Integer> freePartitions = getFreePartitions(originConsumerToPartition, partitions);
		Map<String, List<Integer>> newAssigns = m_assignBalancer.assign(originConsumerToPartition, freePartitions);
		for (Entry<String, List<Integer>> assign : newAssigns.entrySet()) {
			putAssignToResult(result, currentConsumers, assign.getKey(), assign.getValue());
		}

		return result;
	}

	 List<Integer> getFreePartitions(Map<String, List<Integer>> originConsumerToPartition,
	      List<Partition> partitions) {
		List<Integer> freePartitions = new ArrayList<Integer>();
		for (Partition partition : partitions) {
			freePartitions.add(partition.getId());
		}

		for (Entry<String, List<Integer>> assign : originConsumerToPartition.entrySet()) {
			for (Integer partition : assign.getValue()) {
				freePartitions.remove(partition);
			}
		}
		return freePartitions;

	}

	private void putAssignToResult(Map<Integer, Map<String, ClientContext>> result,
	      Map<String, ClientContext> consumerToClientContext, String commonConsumer, List<Integer> newAssign) {
		for (Integer partition : newAssign) {
			Map<String, ClientContext> consumerMap = new HashMap<>();
			consumerMap.put(commonConsumer, consumerToClientContext.get(commonConsumer));

			result.put(partition, consumerMap);
		}
	}

	 Map<String, List<Integer>> mapConsumerToPartitions(List<Partition> currentPartitions,
	      Map<String, ClientContext> currentConsumers, Map<Integer, Map<String, ClientContext>> originAssignment) {
		Map<String, List<Integer>> result = new HashMap<>();
		for (Entry<String, ClientContext> currentConsumer : currentConsumers.entrySet()) {
			result.put(currentConsumer.getKey(), new ArrayList<Integer>());
		}

		HashSet<Integer> partitionIds = new HashSet<>();
		for (Partition partition : currentPartitions) {
			partitionIds.add(partition.getId());
		}

		for (Entry<Integer, Map<String, ClientContext>> entry : originAssignment.entrySet()) {
			// support only has one assignment
			if (!entry.getValue().isEmpty()) {
				if (entry.getValue().size() != 1) {
					log.warn("Partition have more than one consumer assigned");
				}

				String consumer = entry.getValue().keySet().iterator().next();
				int partition = entry.getKey();

				if (partitionIds.contains(partition) && currentConsumers.containsKey((consumer))) {
					result.get(consumer).add(partition);
				}
			}
		}
		return result;
	}
}
