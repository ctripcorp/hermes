package com.ctrip.hermes.metaserver.meta;

import java.util.List;

import com.ctrip.hermes.meta.entity.Server;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaserver.commons.Assignment;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface MetaServerAssigningStrategy {

	public Assignment<String> assign(List<Server> metaServers, List<Topic> topics, Assignment<String> originAssignments);
}
