package com.ctrip.hermes.metaserver.event.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.listen.ListenerContainer;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.x.discovery.ServiceCache;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.ServiceCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.bo.HostPort;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Idc;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Server;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaserver.broker.BrokerAssignmentHolder;
import com.ctrip.hermes.metaserver.cluster.Role;
import com.ctrip.hermes.metaserver.commons.BaseNodeCacheListener;
import com.ctrip.hermes.metaserver.commons.BasePathChildrenCacheListener;
import com.ctrip.hermes.metaserver.commons.ClientContext;
import com.ctrip.hermes.metaserver.commons.EndpointMaker;
import com.ctrip.hermes.metaserver.event.Event;
import com.ctrip.hermes.metaserver.event.EventHandler;
import com.ctrip.hermes.metaserver.event.EventType;
import com.ctrip.hermes.metaserver.event.VersionGuardedTask;
import com.ctrip.hermes.metaserver.meta.MetaHolder;
import com.ctrip.hermes.metaserver.meta.MetaServerAssignmentHolder;
import com.ctrip.hermes.metaservice.service.MetaService;
import com.ctrip.hermes.metaservice.zk.ZKClient;
import com.ctrip.hermes.metaservice.zk.ZKPathUtils;
import com.ctrip.hermes.metaservice.zk.ZKSerializeUtils;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = EventHandler.class, value = "LeaderInitEventHandler")
public class LeaderInitEventHandler extends BaseEventHandler {

	private static final Logger log = LoggerFactory.getLogger(LeaderInitEventHandler.class);

	@Inject
	private SystemClockService m_systemClockService;

	@Inject
	private MetaService m_metaService;

	@Inject
	private MetaHolder m_metaHolder;

	@Inject
	private ZKClient m_zkClient;

	@Inject
	private BrokerAssignmentHolder m_brokerAssignmentHolder;

	@Inject
	private MetaServerAssignmentHolder m_metaServerAssignmentHolder;

	@Inject
	private EndpointMaker m_endpointMaker;

	private ServiceDiscovery<Void> m_serviceDiscovery;

	private ServiceCache<Void> m_serviceCache;

	protected NodeCache m_baseMetaVersionCache;

	protected PathChildrenCache m_metaServerListCache;

	public void setMetaService(MetaService metaService) {
		m_metaService = metaService;
	}

	public void setMetaHolder(MetaHolder metaHolder) {
		m_metaHolder = metaHolder;
	}

	public void setBrokerAssignmentHolder(BrokerAssignmentHolder brokerAssignmentHolder) {
		m_brokerAssignmentHolder = brokerAssignmentHolder;
	}

	public void setMetaServerAssignmentHolder(MetaServerAssignmentHolder metaServerAssignmentHolder) {
		m_metaServerAssignmentHolder = metaServerAssignmentHolder;
	}

	public void setEndpointMaker(EndpointMaker endpointMaker) {
		m_endpointMaker = endpointMaker;
	}

	@Override
	public EventType eventType() {
		return EventType.LEADER_INIT;
	}

	@Override
	public void start() {
		m_serviceDiscovery = ServiceDiscoveryBuilder.builder(Void.class)//
		      .client(m_zkClient.get())//
		      .basePath(ZKPathUtils.getBrokerRegistryBasePath())//
		      .build();

		m_serviceCache = m_serviceDiscovery.serviceCacheBuilder()//
		      .name(ZKPathUtils.getBrokerRegistryName(null))//
		      .threadFactory(HermesThreadFactory.create("brokerDiscoveryCache", true))//
		      .build();

		try {
			m_serviceCache.start();
			m_serviceDiscovery.start();
		} catch (Exception e) {
			throw new RuntimeException("Failed to start broker discovery service", e);
		}

		try {
			m_baseMetaVersionCache = new NodeCache(m_zkClient.get(), ZKPathUtils.getBaseMetaVersionZkPath());
			m_baseMetaVersionCache.start(true);

			m_metaServerListCache = new PathChildrenCache(m_zkClient.get(), ZKPathUtils.getMetaServersZkPath(), true);
			m_metaServerListCache.start(StartMode.BUILD_INITIAL_CACHE);

		} catch (Exception e) {
			throw new RuntimeException(String.format("Init {} failed.", getName()), e);
		}

	}

	@Override
	protected void processEvent(Event event) throws Exception {
		long version = event.getVersion();
		addBaseMetaVersionListener(version);

		Meta baseMeta = loadBaseMeta();

		Server server = getCurServerAndFixStatusByIDC(baseMeta);

		if (server == null || !server.isEnabled()) {
			log.info("Marked down!");
			m_clusterStateHolder.becomeObserver();
			return;
		}

		ArrayList<Topic> topics = new ArrayList<>(baseMeta.getTopics().values());
		List<Server> configedMetaServers = baseMeta.getServers() == null ? new ArrayList<Server>()
		      : new ArrayList<Server>(baseMeta.getServers().values());
		Map<String, Idc> idcs = baseMeta.getIdcs() == null ? new HashMap<String, Idc>() : new HashMap<String, Idc>(
		      baseMeta.getIdcs());

		addMetaServerListListener(version);
		List<Server> metaServers = loadMetaServerList();
		Map<String, ClientContext> brokers = loadAndAddBrokerListListener(new BrokerListChangedListener(version));
		List<Endpoint> configedBrokers = baseMeta.getEndpoints() == null ? new ArrayList<Endpoint>() : new ArrayList<>(
		      baseMeta.getEndpoints().values());

		m_brokerAssignmentHolder.reassign(brokers, configedBrokers, topics, idcs);

		Map<String, Map<Integer, Endpoint>> topicPartition2Endpoint = m_endpointMaker.makeEndpoints(m_eventBus,
		      event.getVersion(), m_clusterStateHolder, m_brokerAssignmentHolder.getAssignments(), false);

		m_metaHolder.setIdcs(idcs);
		m_metaHolder.setConfigedMetaServers(configedMetaServers);
		m_metaHolder.setBaseMeta(baseMeta);
		m_metaHolder.setMetaServers(metaServers);
		m_metaHolder.update(topicPartition2Endpoint);

		m_metaServerAssignmentHolder.reassign(metaServers, m_metaHolder.getConfigedMetaServers(), topics);
	}

	protected void addBaseMetaVersionListener(long version) throws DalException {
		ListenerContainer<NodeCacheListener> listenerContainer = m_baseMetaVersionCache.getListenable();
		listenerContainer.addListener(new BaseMetaVersionListener(version, listenerContainer), m_eventBus.getExecutor());
	}

	private Map<String, ClientContext> loadAndAddBrokerListListener(ServiceCacheListener listener) {
		if (listener != null) {
			m_serviceCache.addListener(listener, m_eventBus.getExecutor());
		}
		List<ServiceInstance<Void>> instances = m_serviceCache.getInstances();
		Map<String, ClientContext> brokers = new HashMap<>();
		for (ServiceInstance<Void> instance : instances) {
			String name = instance.getId();
			String ip = instance.getAddress();
			int port = instance.getPort();
			brokers.put(name, new ClientContext(name, ip, port, null, null, m_systemClockService.now()));
		}
		return brokers;
	}

	protected void addMetaServerListListener(long version) throws Exception {
		ListenerContainer<PathChildrenCacheListener> listenerContainer = m_metaServerListCache.getListenable();
		listenerContainer.addListener(new MetaServerListListener(version, listenerContainer), m_eventBus.getExecutor());
	}

	private List<Server> loadMetaServerList() {
		List<Server> metaServers = new ArrayList<>();
		for (ChildData childData : m_metaServerListCache.getCurrentData()) {
			HostPort hostPort = ZKSerializeUtils.deserialize(childData.getData(), HostPort.class);

			Server s = new Server();
			s.setHost(hostPort.getHost());
			s.setId(ZKPathUtils.lastSegment(childData.getPath()));
			s.setPort(hostPort.getPort());

			metaServers.add(s);
		}

		return metaServers;
	}

	private Meta loadBaseMeta() throws Exception {
		return m_metaService.refreshMeta();
	}

	@Override
	protected Role role() {
		return Role.LEADER;
	}

	protected class BaseMetaVersionListener extends BaseNodeCacheListener {

		protected BaseMetaVersionListener(long version, ListenerContainer<NodeCacheListener> listenerContainer) {
			super(version, listenerContainer);
		}

		@Override
		protected void processNodeChanged() {
			try {
				Long baseMetaVersion = ZKSerializeUtils.deserialize(m_baseMetaVersionCache.getCurrentData().getData(),
				      Long.class);
				log.info("Leader BaseMeta changed(version:{}).", baseMetaVersion);
				m_eventBus.pubEvent(new com.ctrip.hermes.metaserver.event.Event(EventType.BASE_META_CHANGED, m_version,
				      baseMetaVersion));
			} catch (Exception e) {
				log.error("Exception occurred while handling base meta watcher event.", e);
			}
		}

		@Override
		protected String getName() {
			return "[Leader]BaseMetaVersionListener";
		}
	}

	protected class MetaServerListListener extends BasePathChildrenCacheListener {

		protected MetaServerListListener(long version, ListenerContainer<PathChildrenCacheListener> listenerContainer) {
			super(version, listenerContainer);
		}

		@Override
		protected void childChanged() {
			try {
				List<Server> metaServers = loadMetaServerList();

				m_eventBus.pubEvent(new com.ctrip.hermes.metaserver.event.Event(EventType.META_SERVER_LIST_CHANGED,
				      m_version, metaServers));
			} catch (Exception e) {
				log.error("Exception occurred while handling meta server list watcher event.", e);
			}
		}

		@Override
		protected String getName() {
			return "[Leader]MetaServerListListener";
		}

	}

	private class BrokerListChangedListener implements ServiceCacheListener {

		private long m_version;

		public BrokerListChangedListener(long version) {
			m_version = version;
		}

		@Override
		public void stateChanged(CuratorFramework client, ConnectionState newState) {
			// do nothing
		}

		@Override
		public void cacheChanged() {
			try {
				new VersionGuardedTask(m_version) {

					@Override
					public String name() {
						return "[Leader]BrokerListChangedListener";
					}

					@Override
					protected void doRun() {
						Map<String, ClientContext> brokerList = loadAndAddBrokerListListener(null);
						m_eventBus.pubEvent(new Event(EventType.BROKER_LIST_CHANGED, m_version, brokerList));
					}

					@Override
					protected void onGuardNotPass() {
						m_serviceCache.removeListener(BrokerListChangedListener.this);
					}
				}.run();
			} catch (Exception e) {
				log.error("Exception occurred while handling BrokerListChangedListener", e);
			}

		}
	}

}
