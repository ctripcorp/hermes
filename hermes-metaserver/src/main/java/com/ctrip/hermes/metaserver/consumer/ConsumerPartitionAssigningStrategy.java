package com.ctrip.hermes.metaserver.consumer;

import java.util.List;
import java.util.Map;

import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.metaserver.commons.ClientContext;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface ConsumerPartitionAssigningStrategy {

	public Map<Integer, Map<String, ClientContext>> assign(List<Partition> partitions,
	      Map<String, ClientContext> consumers, Map<Integer, Map<String, ClientContext>> originAssignment);
}
