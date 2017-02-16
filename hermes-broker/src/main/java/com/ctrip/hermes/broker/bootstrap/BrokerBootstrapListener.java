package com.ctrip.hermes.broker.bootstrap;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class BrokerBootstrapListener implements ServletContextListener {
	private static final Logger log = LoggerFactory.getLogger(BrokerBootstrapListener.class);

	@Override
	public void contextInitialized(ServletContextEvent sce) {
		try {
			PlexusComponentLocator.lookup(BrokerBootstrap.class).start();
		} catch (Exception e) {
			throw new RuntimeException("Fail to start broker.", e);
		}
	}

	@Override
	public void contextDestroyed(ServletContextEvent sce) {
		stopBroker();
		HermesThreadFactory.waitAllShutdown(60000);// wait for 60 seconds
	}

	private void stopBroker() {
		try {
			PlexusComponentLocator.lookup(BrokerBootstrap.class).stop();
		} catch (Exception e) {
			log.error("Exception occurred while stopping broker", e);
		}
	}

}
