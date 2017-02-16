package com.ctrip.hermes.core.transport.endpoint;

import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Endpoint;

@Named(type = EndpointClient.class)
public class DefaultEndpointClient extends AbstractEndpointClient {

	@Override
	protected boolean isEndpointValid(Endpoint endpoint) {
		return PlexusComponentLocator.lookup(EndpointManager.class).containsEndpoint(endpoint);
	}
}
