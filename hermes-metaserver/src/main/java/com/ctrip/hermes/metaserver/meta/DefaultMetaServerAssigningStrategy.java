package com.ctrip.hermes.metaserver.meta;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Server;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaserver.commons.Assignment;
import com.ctrip.hermes.metaserver.commons.ClientContext;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = MetaServerAssigningStrategy.class)
public class DefaultMetaServerAssigningStrategy implements MetaServerAssigningStrategy {

	@Override
	public Assignment<String> assign(List<Server> metaServers, List<Topic> topics, Assignment<String> originAssignments) {
		Assignment<String> newAssignments = new Assignment<>();

		if (metaServers != null) {

			int metaServerPos = 0;
			int metaServerCount = metaServers.size();

			/**
			 * add "&& metaServerCount > 0". If there is no metaServers running.
			 */
			if (topics != null && metaServerCount > 0) {
				for (Topic topic : topics) {
					if (Endpoint.BROKER.equals(topic.getEndpointType())) {
						Server metaServer = metaServers.get(metaServerPos);
						metaServerPos = (metaServerPos + 1) % metaServerCount;
						Map<String, ClientContext> server = new HashMap<>();
						server.put(metaServer.getId(),
						      new ClientContext(metaServer.getId(), metaServer.getHost(), metaServer.getPort(), null,
						            metaServer.getIdc(), -1));
						newAssignments.addAssignment(topic.getName(), server);
					}
				}
			}
		}

		return newAssignments;
	}
}
