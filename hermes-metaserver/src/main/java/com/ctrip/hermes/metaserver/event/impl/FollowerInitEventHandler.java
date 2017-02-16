package com.ctrip.hermes.metaserver.event.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.curator.framework.listen.ListenerContainer;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.meta.entity.Idc;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Server;
import com.ctrip.hermes.metaserver.broker.BrokerAssignmentHolder;
import com.ctrip.hermes.metaserver.cluster.ClusterStateHolder;
import com.ctrip.hermes.metaserver.cluster.Role;
import com.ctrip.hermes.metaserver.commons.BaseNodeCacheListener;
import com.ctrip.hermes.metaserver.event.Event;
import com.ctrip.hermes.metaserver.event.EventHandler;
import com.ctrip.hermes.metaserver.event.EventType;
import com.ctrip.hermes.metaserver.event.VersionGuardedTask;
import com.ctrip.hermes.metaserver.meta.MetaHolder;
import com.ctrip.hermes.metaserver.meta.MetaInfo;
import com.ctrip.hermes.metaserver.meta.MetaServerAssignmentHolder;
import com.ctrip.hermes.metaservice.service.MetaService;
import com.ctrip.hermes.metaservice.zk.ZKClient;
import com.ctrip.hermes.metaservice.zk.ZKPathUtils;
import com.ctrip.hermes.metaservice.zk.ZKSerializeUtils;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = EventHandler.class, value = "FollowerInitEventHandler")
public class FollowerInitEventHandler extends BaseEventHandler {

	private static final Logger log = LoggerFactory.getLogger(FollowerInitEventHandler.class);

	@Inject
	protected MetaHolder m_metaHolder;

	@Inject
	protected BrokerAssignmentHolder m_brokerAssignmentHolder;

	@Inject
	protected MetaServerAssignmentHolder m_metaServerAssignmentHolder;

	@Inject
	protected ZKClient m_zkClient;

	@Inject
	protected LeaderMetaFetcher m_leaderMetaFetcher;

	@Inject
	protected MetaService m_metaService;

	protected ScheduledExecutorService m_scheduledExecutor;

	protected NodeCache m_baseMetaVersionCache;

	protected NodeCache m_leaderMetaVersionCache;

	public void setMetaHolder(MetaHolder metaHolder) {
		m_metaHolder = metaHolder;
	}

	public void setBrokerAssignmentHolder(BrokerAssignmentHolder brokerAssignmentHolder) {
		m_brokerAssignmentHolder = brokerAssignmentHolder;
	}

	public void setMetaServerAssignmentHolder(MetaServerAssignmentHolder metaServerAssignmentHolder) {
		m_metaServerAssignmentHolder = metaServerAssignmentHolder;
	}

	public void setZkClient(ZKClient zkClient) {
		m_zkClient = zkClient;
	}

	public void setLeaderMetaFetcher(LeaderMetaFetcher leaderMetaFetcher) {
		m_leaderMetaFetcher = leaderMetaFetcher;
	}

	@Override
	public EventType eventType() {
		return EventType.FOLLOWER_INIT;
	}

	public void start() {
		startScheduledExecutor();
		try {
			m_baseMetaVersionCache = new NodeCache(m_zkClient.get(), ZKPathUtils.getBaseMetaVersionZkPath());
			m_baseMetaVersionCache.start(true);

			m_leaderMetaVersionCache = new NodeCache(m_zkClient.get(), ZKPathUtils.getMetaInfoZkPath());
			m_leaderMetaVersionCache.start(true);
		} catch (Exception e) {
			throw new RuntimeException(String.format("Init {} failed.", getName()), e);
		}

	}

	protected void startScheduledExecutor() {
	   m_scheduledExecutor = Executors.newSingleThreadScheduledExecutor(HermesThreadFactory
		      .create("FollowerRetry", true));
   }

	@Override
	protected void processEvent(Event event) throws Exception {
		long version = event.getVersion();

		m_brokerAssignmentHolder.clear();

		addBaseMetaVersionListener(version);
		if (!fetchBaseMetaAndRoleChangeIfNeeded(m_clusterStateHolder)) {
			loadAndAddLeaderMetaVersionListener(version);
		}
	}

	protected void addBaseMetaVersionListener(long version) throws DalException {
		ListenerContainer<NodeCacheListener> listenerContainer = m_baseMetaVersionCache.getListenable();
		listenerContainer.addListener(new BaseMetaVersionListener(version, listenerContainer), m_eventBus.getExecutor());
	}

	protected void loadAndAddLeaderMetaVersionListener(long version) {
		ListenerContainer<NodeCacheListener> listenerContainer = m_leaderMetaVersionCache.getListenable();
		listenerContainer
		      .addListener(new LeaderMetaVersionListener(version, listenerContainer), m_eventBus.getExecutor());
		loadLeaderMeta(version);
	}

	protected void loadLeaderMeta(final long version) {
		MetaInfo metaInfo = ZKSerializeUtils.deserialize(m_leaderMetaVersionCache.getCurrentData().getData(),
		      MetaInfo.class);
		Meta meta = m_leaderMetaFetcher.fetchMetaInfo(metaInfo);

		if (meta != null && (m_metaHolder.getMeta() == null || meta.getVersion() != m_metaHolder.getMeta().getVersion())) {
			m_metaHolder.setMeta(meta);
			log.info("[{}]Fetched meta from leader(endpoint={}:{},version={})", role(), metaInfo.getHost(),
			      metaInfo.getPort(), meta.getVersion());
		} else if (meta == null) {
			delayRetry(m_scheduledExecutor, new VersionGuardedTask(version) {

				@Override
				public void doRun() throws Exception {
					loadLeaderMeta(version);
				}

				@Override
				public String name() {
					return String.format("[%s]LeaderMetaVersionListenerRetry", role());
				}
			});
		}
	}

	protected boolean fetchBaseMetaAndRoleChangeIfNeeded(ClusterStateHolder clusterStateHolder) throws DalException {
		Meta baseMeta = m_metaService.refreshMeta();
		log.info("[{}]BaseMeta refreshed(id:{}, version:{}).", role(), baseMeta.getId(), baseMeta.getVersion());

		List<Server> configedMetaServers = baseMeta.getServers() == null ? new ArrayList<Server>()
		      : new ArrayList<Server>(baseMeta.getServers().values());
		Map<String, Idc> idcs = baseMeta.getIdcs() == null ? new HashMap<String, Idc>() : new HashMap<String, Idc>(
		      baseMeta.getIdcs());

		m_metaHolder.setConfigedMetaServers(configedMetaServers);
		m_metaHolder.setIdcs(idcs);

		return roleChanged(baseMeta, clusterStateHolder);
	}

	protected boolean roleChanged(Meta baseMeta, ClusterStateHolder clusterStateHolder) {
		Server server = getCurServerAndFixStatusByIDC(baseMeta);

		if (server == null || !server.isEnabled()) {
			log.info("[{}]Marked down!", role());
			clusterStateHolder.becomeObserver();
			return true;
		}

		return false;
	}

	@Override
	protected Role role() {
		return Role.FOLLOWER;
	}

	protected class BaseMetaVersionListener extends BaseNodeCacheListener {

		protected BaseMetaVersionListener(long version, ListenerContainer<NodeCacheListener> listenerContainer) {
			super(version, listenerContainer);
		}

		@Override
		protected void processNodeChanged() {
			try {
				fetchBaseMetaAndRoleChangeIfNeeded(m_clusterStateHolder);
			} catch (Exception e) {
				log.error("[{}]Exception occurred while doing BaseMetaVersionListener.processNodeChanged, will retry.",
				      role(), e);

				delayRetry(m_scheduledExecutor, new VersionGuardedTask(m_version) {

					@Override
					public String name() {
						return String.format("[%s]BaseMetaVersionListener", role());
					}

					@Override
					protected void doRun() throws Exception {
						processNodeChanged();
					}

					@Override
					protected void onGuardNotPass() {
						removeListener();
					}
				});
			}
		}

		@Override
		protected String getName() {
			return "BaseMetaVersionListener";
		}
	}

	protected class LeaderMetaVersionListener extends BaseNodeCacheListener {

		protected LeaderMetaVersionListener(long version, ListenerContainer<NodeCacheListener> listenerContainer) {
			super(version, listenerContainer);
		}

		@Override
		protected void processNodeChanged() {
			try {
				loadLeaderMeta(m_version);
			} catch (Exception e) {
				log.error("[{}]Exception occurred while handling leader meta watcher event.", role(), e);
			}
		}

		@Override
		protected String getName() {
			return String.format("[%s]LeaderMetaVersionListener", role());
		}
	}

}
