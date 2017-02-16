package com.ctrip.hermes.consumer.engine.bootstrap;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Named;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = ConsumerBootstrapRegistry.class)
public class DefaultConsumerBootstrapRegistry extends ContainerHolder implements Initializable,
      ConsumerBootstrapRegistry {

	private Map<String, ConsumerBootstrap> m_bootstraps = new ConcurrentHashMap<String, ConsumerBootstrap>();

	@Override
	public void initialize() throws InitializationException {
		Map<String, ConsumerBootstrap> bootstraps = lookupMap(ConsumerBootstrap.class);

		for (Map.Entry<String, ConsumerBootstrap> entry : bootstraps.entrySet()) {
			m_bootstraps.put(entry.getKey(), entry.getValue());
		}
	}

	@Override
	public void registerBootstrap(String endpointType, ConsumerBootstrap consumerBootstrap) {
		if (m_bootstraps.containsKey(endpointType)) {
			throw new IllegalArgumentException(String.format(
			      "ConsumerBootstrap for endpoint type %s is already registered", endpointType));
		}

		m_bootstraps.put(endpointType, consumerBootstrap);
	}

	@Override
	public ConsumerBootstrap findConsumerBootstrap(String endpointType) {
		return m_bootstraps.get(endpointType);
	}

}
