package com.ctrip.hermes.metaserver.event.impl;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.meta.entity.Idc;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Server;
import com.ctrip.hermes.metaserver.cluster.ClusterStateHolder;
import com.ctrip.hermes.metaserver.cluster.Role;
import com.ctrip.hermes.metaserver.config.MetaServerConfig;
import com.ctrip.hermes.metaserver.event.Event;
import com.ctrip.hermes.metaserver.event.EventBus;
import com.ctrip.hermes.metaserver.event.EventHandler;
import com.ctrip.hermes.metaserver.event.Task;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public abstract class BaseEventHandler implements EventHandler {

	private static final Logger log = LoggerFactory.getLogger(BaseEventHandler.class);

	@Inject
	protected MetaServerConfig m_config;

	protected EventBus m_eventBus;

	protected ClusterStateHolder m_clusterStateHolder;

	@Override
	public void onEvent(Event event) throws Exception {
		if (m_clusterStateHolder.getRole() != role()) {
			return;
		}

		processEvent(event);
	}

	@Override
	public void setClusterStateHolder(ClusterStateHolder clusterStateHolder) {
		m_clusterStateHolder = clusterStateHolder;
	}

	@Override
	public void setEventBus(EventBus eventBus) {
		m_eventBus = eventBus;
	}

	@Override
	public String getName() {
		return this.getClass().getSimpleName();
	}

	@Override
	public void start() {

	}

	protected abstract void processEvent(Event event) throws Exception;

	protected abstract Role role();

	protected Server getCurServerAndFixStatusByIDC(Meta baseMeta) {
		if (baseMeta.getServers() != null && !baseMeta.getServers().isEmpty()) {
			for (Server server : baseMeta.getServers().values()) {
				if (m_config.getMetaServerHost().equals(server.getHost())
				      && m_config.getMetaServerPort() == server.getPort()) {
					Server tmpServer = new Server();
					tmpServer.setHost(server.getHost());
					tmpServer.setId(server.getId());
					tmpServer.setPort(server.getPort());
					tmpServer.setIdc(server.getIdc());
					Idc idc = baseMeta.findIdc(server.getIdc());
					if (idc != null) {
						tmpServer.setEnabled(idc.isEnabled() ? server.isEnabled() : false);
						log.info("MetaServer's enabled is {}(idc:{}, server:{}).", tmpServer.isEnabled(), idc.isEnabled(),
						      server.isEnabled());
					} else {
						tmpServer.setEnabled(false);
						log.info("MetaServer's idc not found(idc={}), will be mark down.", server.getIdc());
					}
					return tmpServer;
				}
			}
		}

		log.info("MetaServer not found in baseMeta, will be marked down.");

		return null;
	}

	protected void delayRetry(ScheduledExecutorService executor, final Task task) {
		executor.schedule(new DelayRetryTaskRunner(task), 2, TimeUnit.SECONDS);
	}

	private class DelayRetryTaskRunner implements Runnable {

		private Task m_task;

		public DelayRetryTaskRunner(Task task) {
			m_task = task;
		}

		@Override
		public void run() {
			m_eventBus.getExecutor().submit(new Runnable() {

				@Override
				public void run() {
					try {
						m_task.run();
					} catch (Exception e) {
						log.error("Excpetion occurred while running delay retry task {}", m_task.name());
					}
				}
			});
		}
	}
}
