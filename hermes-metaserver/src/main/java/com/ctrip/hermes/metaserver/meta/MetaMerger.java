package com.ctrip.hermes.metaserver.meta;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.core.utils.CollectionUtil;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Server;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.meta.entity.ZookeeperEnsemble;
import com.ctrip.hermes.meta.transform.DefaultSaxParser;
import com.ctrip.hermes.metaserver.commons.Constants;

public class MetaMerger {

	private final static Logger log = LoggerFactory.getLogger(MetaMerger.class);

	public Meta merge(Meta base, List<Server> newServers, Map<String, Map<Integer, Endpoint>> newPartition2Endpoint,
	      List<ZookeeperEnsemble> zookeeperEnsembles) {
		Meta newMeta;
		try {
			newMeta = DefaultSaxParser.parse(base.toString());
		} catch (Exception e) {
			throw new RuntimeException("Error parse Meta xml", e);
		}

		if (CollectionUtil.isNotEmpty(newServers)) {
			newMeta.getServers().clear();
			for (Server s : newServers) {
				newMeta.addServer(s);
			}
		}
		if (CollectionUtil.isNotEmpty(zookeeperEnsembles)) {
			newMeta.getZookeeperEnsembles().clear();
			for (ZookeeperEnsemble zookeeperEnsemble : zookeeperEnsembles) {
				newMeta.getZookeeperEnsembles().put(zookeeperEnsemble.getId(), zookeeperEnsemble);
			}
		}

		if (CollectionUtil.isNotEmpty(newPartition2Endpoint)) {
			removeBrokerEndpoints(newMeta);

			for (Map.Entry<String, Map<Integer, Endpoint>> topicEntry : newPartition2Endpoint.entrySet()) {
				Topic topic = newMeta.findTopic(topicEntry.getKey());

				for (Map.Entry<Integer, Endpoint> partitionEntry : topicEntry.getValue().entrySet()) {
					Endpoint endpoint = partitionEntry.getValue();
					if (endpoint != null) {
						Endpoint existingEndpoint = newMeta.findEndpoint(endpoint.getId());
						if (existingEndpoint == null) {
							newMeta.addEndpoint(endpoint);
						} else {
							if (Constants.ENDPOINT_GROUP_ASSIGNMENT_CHANGING.equals(existingEndpoint.getGroup())
							      && !Constants.ENDPOINT_GROUP_ASSIGNMENT_CHANGING.equals(endpoint.getGroup())) {
								newMeta.addEndpoint(endpoint);
							}
						}
					}

					int partitionId = partitionEntry.getKey();
					Partition p = topic.findPartition(partitionId);
					if (p == null) {
						log.warn("partition {} not found in topic {}, ignore", partitionId, topic);
					} else {
						if (endpoint != null) {
							p.setEndpoint(endpoint.getId());
						} else {
							p.setEndpoint(null);
						}
					}
				}
			}
		}

		return newMeta;

	}

	private void removeBrokerEndpoints(Meta base) {
		Map<String, Endpoint> endpoints = base.getEndpoints();
		Iterator<Entry<String, Endpoint>> iter = endpoints.entrySet().iterator();
		while (iter.hasNext()) {
			Entry<String, Endpoint> entry = iter.next();
			if (Endpoint.BROKER.equals(entry.getValue().getType())) {
				iter.remove();
			}
		}
	}

}
