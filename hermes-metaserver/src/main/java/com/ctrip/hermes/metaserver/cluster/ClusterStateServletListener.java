package com.ctrip.hermes.metaserver.cluster;

import java.util.concurrent.TimeUnit;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.metaserver.broker.BrokerLeaseHolder;
import com.ctrip.hermes.metaserver.consumer.ConsumerLeaseHolder;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class ClusterStateServletListener implements ServletContextListener {
	private static final Logger log = LoggerFactory.getLogger(ClusterStateServletListener.class);

	@Override
	public void contextInitialized(ServletContextEvent sce) {
		try {
			PlexusComponentLocator.lookup(ClusterStateHolder.class).start();
		} catch (Exception e) {
			log.error("Failed to start ClusterStatusHolder.", e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public void contextDestroyed(ServletContextEvent sce) {
		try {
			ClusterStateHolder clusterStateHolder = PlexusComponentLocator.lookup(ClusterStateHolder.class);

			clusterStateHolder.setLeaseAssigning(false);

			PlexusComponentLocator.lookup(BrokerLeaseHolder.class).close();
			PlexusComponentLocator.lookup(ConsumerLeaseHolder.class).close();

			TimeUnit.MILLISECONDS.sleep(Math.max(BrokerLeaseHolder.LEASE_SYNC_INTERVAL_MILLIS, ConsumerLeaseHolder.LEASE_SYNC_INTERVAL_MILLIS));

			clusterStateHolder.close();
		} catch (Exception e) {
			log.error("Failed to close ClusterStatusHolder.", e);
			throw new RuntimeException(e);
		}
	}

}
