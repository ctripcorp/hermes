package com.ctrip.hermes.core.transport.endpoint;

import com.ctrip.hermes.meta.entity.Endpoint;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface EndpointManager {

	Endpoint getEndpoint(String topic, int partition);

	void updateEndpoint(String topic, int partition, Endpoint newEndpoint);

	void refreshEndpoint(String topic, int partition);

	boolean containsEndpoint(Endpoint endpoint);

}
