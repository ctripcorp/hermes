package com.ctrip.hermes.metaserver.fulltest;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.unidal.lookup.ContainerLoader;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.env.ClientEnvironment;
import com.ctrip.hermes.metaserver.config.MetaServerConfig;
import com.ctrip.hermes.metaservice.service.MetaService;

/**
 * used in HermesClassLoaderMetaServer: Class clazz = parent.loadClass("com.ctrip.hermes.metaserver.fulltest.LoadMocksListener");
 */
public class LoadMocksListener extends MetaServerBaseTest implements ServletContextListener {

	int port;

	public void setPort(int port) {
		this.port = port;
	}

	@Override
	public void contextInitialized(ServletContextEvent servletContextEvent) {
		try {
			initPlexus();
			// for mock
			defineComponent(MetaServerConfig.class, MockMetaServerConfig.class).req(ClientEnvironment.class);
			defineComponent(MetaService.class, MockMetaService.class);
			defineComponent(ClientEnvironment.class, MockClientEnviroment.class);

			// end of mock
			MockMetaServerConfig config = (MockMetaServerConfig) PlexusComponentLocator.lookup(MetaServerConfig.class);
			config.setPort(port);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void contextDestroyed(ServletContextEvent servletContextEvent) {
		ContainerLoader.destroyDefaultContainer();
	}
}
