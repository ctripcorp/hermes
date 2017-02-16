package com.ctrip.hermes.metaserver.consumer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.metaserver.commons.ClientContext;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = ConsumerPartitionAssigningStrategy.class)
public class DefaultConsumerPartitionAssigningStrategy implements ConsumerPartitionAssigningStrategy {

	@Override
	public Map<Integer, Map<String, ClientContext>> assign(List<Partition> partitions,
	      Map<String, ClientContext> consumers, Map<Integer, Map<String, ClientContext>> originAssignment) {
		Map<Integer, Map<String, ClientContext>> result = new HashMap<>();

		if (partitions == null || partitions.isEmpty() || consumers == null || consumers.isEmpty()) {
			return result;
		}
		ArrayList<Entry<String, ClientContext>> consumerEntries = new ArrayList<>(consumers.entrySet());
		int consumerCount = consumerEntries.size();

		for (Partition partition : partitions) {
			result.put(partition.getId(), new HashMap<String, ClientContext>());
		}

		int consumerPos = 0;
		for (Partition partition : partitions) {
			Entry<String, ClientContext> entry = consumerEntries.get(consumerPos);
			result.get(partition.getId()).put(entry.getKey(), entry.getValue());
			consumerPos = (consumerPos + 1) % consumerCount;
		}

		return result;
	}
}
