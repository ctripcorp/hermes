package com.ctrip.hermes.metaserver.broker;

import java.util.List;
import java.util.Map;

import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaserver.commons.Assignment;
import com.ctrip.hermes.metaserver.commons.ClientContext;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface BrokerPartitionAssigningStrategy {

	public Map<String, Assignment<Integer>> assign(Map<String, ClientContext> brokers, List<Topic> topics,
	      Map<String, Assignment<Integer>> originAssignments);
}
