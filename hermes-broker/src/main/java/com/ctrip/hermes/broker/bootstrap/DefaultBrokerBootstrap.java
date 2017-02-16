package com.ctrip.hermes.broker.bootstrap;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.broker.lease.BrokerLeaseContainer;
import com.ctrip.hermes.broker.registry.BrokerRegistry;
import com.ctrip.hermes.broker.shutdown.ShutdownRequestMonitor;
import com.ctrip.hermes.broker.transport.NettyServer;
import com.ctrip.hermes.env.config.broker.BrokerConfigProvider;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = BrokerBootstrap.class)
public class DefaultBrokerBootstrap extends ContainerHolder implements BrokerBootstrap {

	private static final Logger log = LoggerFactory.getLogger(DefaultBrokerBootstrap.class);

	@Inject
	private NettyServer m_nettyServer;

	@Inject
	private BrokerConfigProvider m_config;

	@Inject
	private BrokerRegistry m_registry;

	@Inject
	private ShutdownRequestMonitor m_shutdownReqMonitor;

	@Inject
	private BrokerLeaseContainer m_leaseContainer;

	@Override
	public void start() throws Exception {
		log.info("Starting broker...");
		ChannelFuture future = m_nettyServer.start(m_config.getListeningPort());

		future.addListener(new ChannelFutureListener() {

			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				if (future.isSuccess()) {
					m_shutdownReqMonitor.start();
					m_registry.start();
					log.info("Broker started at port {} with name {}.", m_config.getListeningPort(), m_config.getSessionId());
				} else {
					log.error("Failed to start broker.");
				}

			}
		});
	}

	@Override
	public void stop() throws Exception {
		m_registry.stop();

		int allLeaseExpiredCount = 0;

		long timeout = System.currentTimeMillis() + 30 * 1000L;

		while (!Thread.interrupted() && System.currentTimeMillis() <= timeout) {
			if (m_leaseContainer.isAllLeaseExpired()) {
				allLeaseExpiredCount++;
				if (allLeaseExpiredCount >= 3) {
					break;
				}
			} else {
				allLeaseExpiredCount = 0;
			}

			TimeUnit.SECONDS.sleep(1);
		}

		m_shutdownReqMonitor.stopBroker();
		log.info("Broker stopped...");
	}

}
