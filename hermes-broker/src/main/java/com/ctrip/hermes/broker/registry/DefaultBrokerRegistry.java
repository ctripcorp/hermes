package com.ctrip.hermes.broker.registry;

import java.io.IOException;

import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.net.Networks;

import com.ctrip.hermes.broker.zk.ZKClient;
import com.ctrip.hermes.env.config.broker.BrokerConfigProvider;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = BrokerRegistry.class)
public class DefaultBrokerRegistry implements BrokerRegistry, Initializable {

	@Inject
	private BrokerConfigProvider m_config;

	@Inject
	private ZKClient m_client;

	private ServiceDiscovery<Void> m_serviceDiscovery;

	private ServiceInstance<Void> thisInstance;

	@Override
	public void initialize() throws InitializationException {

		try {
			thisInstance = ServiceInstance.<Void> builder()//
			      .name(m_config.getRegistryName(null))//
			      .address(Networks.forIp().getLocalHostAddress())//
			      .port(m_config.getListeningPort())//
			      .id(m_config.getSessionId())//
			      .build();

			m_serviceDiscovery = ServiceDiscoveryBuilder.builder(Void.class)//
			      .client(m_client.get())//
			      .basePath(m_config.getRegistryBasePath())//
			      .thisInstance(thisInstance)//
			      .build();
		} catch (Exception e) {
			throw new InitializationException("Failed to init broker registry.", e);
		}

	}

	@Override
	public void start() throws Exception {
		m_serviceDiscovery.start();
	}

	@Override
	public void stop() throws IOException {
		m_serviceDiscovery.close();
	}

}
