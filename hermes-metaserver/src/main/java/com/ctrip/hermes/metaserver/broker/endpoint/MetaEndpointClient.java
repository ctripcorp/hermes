package com.ctrip.hermes.metaserver.broker.endpoint;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.transport.endpoint.AbstractEndpointClient;
import com.ctrip.hermes.core.transport.endpoint.EndpointClient;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.metaserver.meta.MetaHolder;

@Named(type = EndpointClient.class, value = MetaEndpointClient.ID)
public class MetaEndpointClient extends AbstractEndpointClient {
	public static final String ID = "meta";

	@Inject
	private MetaHolder m_metaHolder;

	@Override
	protected boolean isEndpointValid(Endpoint endpoint) {
		return m_metaHolder.getMeta().findEndpoint(endpoint.getId()) != null;
	}
}
