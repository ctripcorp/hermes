package com.ctrip.hermes.metaserver.event.impl;

import java.util.List;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.meta.entity.Server;
import com.ctrip.hermes.metaserver.cluster.Role;
import com.ctrip.hermes.metaserver.event.Event;
import com.ctrip.hermes.metaserver.event.EventHandler;
import com.ctrip.hermes.metaserver.event.EventType;
import com.ctrip.hermes.metaserver.meta.MetaHolder;
import com.ctrip.hermes.metaserver.meta.MetaServerAssignmentHolder;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = EventHandler.class, value = "MetaServerListChangedEventHandler")
public class MetaServerListChangedEventHandler extends BaseEventHandler {

	@Inject
	private MetaHolder m_metaHolder;

	@Inject
	private MetaServerAssignmentHolder m_metaServerAssignmentHolder;

	@Override
	public EventType eventType() {
		return EventType.META_SERVER_LIST_CHANGED;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void processEvent(Event event) throws Exception {
		Object data = event.getData();
		if (data != null) {
			List<Server> metaServers = (List<Server>) data;
			m_metaHolder.update(metaServers);
			m_metaServerAssignmentHolder.reassign(metaServers, m_metaHolder.getConfigedMetaServers(), null);
		}
	}

	@Override
	protected Role role() {
		return Role.LEADER;
	}

}
