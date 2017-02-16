package com.ctrip.hermes.metaserver.event.impl;

import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Server;
import com.ctrip.hermes.metaserver.cluster.ClusterStateHolder;
import com.ctrip.hermes.metaserver.cluster.Role;
import com.ctrip.hermes.metaserver.event.EventHandler;
import com.ctrip.hermes.metaserver.event.EventType;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = EventHandler.class, value = "ObserverInitEventHandler")
public class ObserverInitEventHandler extends FollowerInitEventHandler {

	private static final Logger log = LoggerFactory.getLogger(ObserverInitEventHandler.class);

	@Override
	public EventType eventType() {
		return EventType.OBSERVER_INIT;
	}

	@Override
	protected void startScheduledExecutor() {
		m_scheduledExecutor = Executors.newSingleThreadScheduledExecutor(HermesThreadFactory
		      .create("ObserverRetry", true));
	}

	protected boolean roleChanged(Meta baseMeta, ClusterStateHolder clusterStateHolder) {
		Server server = getCurServerAndFixStatusByIDC(baseMeta);

		if (server != null && server.isEnabled()) {
			log.info("[{}]Marked up!", role());
			clusterStateHolder.becomeFollower();
			return true;
		} else {
			log.info("[{}]Still marked down!", role());
			return false;
		}
	}

	@Override
	protected Role role() {
		return Role.OBSERVER;
	}
}
