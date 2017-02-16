package com.ctrip.hermes.metaserver.event.impl;

import java.util.Map;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.metaserver.broker.BrokerAssignmentHolder;
import com.ctrip.hermes.metaserver.cluster.Role;
import com.ctrip.hermes.metaserver.commons.ClientContext;
import com.ctrip.hermes.metaserver.commons.EndpointMaker;
import com.ctrip.hermes.metaserver.event.Event;
import com.ctrip.hermes.metaserver.event.EventHandler;
import com.ctrip.hermes.metaserver.event.EventType;
import com.ctrip.hermes.metaserver.meta.MetaHolder;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = EventHandler.class, value = "BrokerListChangedEventHandler")
public class BrokerListChangedEventHandler extends BaseEventHandler {

	@Inject
	private BrokerAssignmentHolder m_brokerAssignmentHolder;

	@Inject
	private EndpointMaker m_endpointMaker;

	@Inject
	private MetaHolder m_metaHolder;

	@Override
	public EventType eventType() {
		return EventType.BROKER_LIST_CHANGED;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void processEvent(Event event) throws Exception {
		Object data = event.getData();
		if (data != null) {
			Map<String, ClientContext> brokers = (Map<String, ClientContext>) data;

			m_brokerAssignmentHolder.reassign(brokers, m_metaHolder.getIdcs());
			m_metaHolder.update(m_endpointMaker.makeEndpoints(m_eventBus, event.getVersion(), m_clusterStateHolder,
			      m_brokerAssignmentHolder.getAssignments(), false));
		}
	}

	@Override
	protected Role role() {
		return Role.LEADER;
	}

}
