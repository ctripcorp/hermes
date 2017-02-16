package com.ctrip.hermes.metaserver.meta;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.net.Networks;
import org.unidal.tuple.Pair;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.bo.ClientMeta;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Idc;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Server;
import com.ctrip.hermes.metaserver.cluster.ClusterStateHolder;
import com.ctrip.hermes.metaserver.cluster.Role;
import com.ctrip.hermes.metaserver.commons.MetaUtils;
import com.ctrip.hermes.metaserver.config.MetaServerConfig;
import com.ctrip.hermes.metaserver.log.LoggerConstants;
import com.ctrip.hermes.metaservice.service.ZookeeperService;
import com.ctrip.hermes.metaservice.zk.ZKClient;
import com.ctrip.hermes.metaservice.zk.ZKPathUtils;
import com.ctrip.hermes.metaservice.zk.ZKSerializeUtils;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = MetaHolder.class)
public class MetaHolder implements Initializable {

	private static final Logger log = LoggerFactory.getLogger(MetaHolder.class);

	private static final Logger traceLog = LoggerFactory.getLogger(LoggerConstants.TRACE);

	@Inject
	private MetaServerConfig m_config;

	@Inject
	private ZKClient m_zkClient;

	@Inject
	private ZookeeperService m_zkService;

	private AtomicReference<MetaCache> m_mergedMetaCache = new AtomicReference<>();

	private AtomicReference<Meta> m_baseCache = new AtomicReference<>();

	private AtomicReference<List<Server>> m_metaServerListCache = new AtomicReference<>();

	private AtomicReference<Map<String, Idc>> m_idcs;

	private AtomicReference<Map<Pair<String, Integer>, Server>> m_configedMetaServers;

	// topic -> partition -> endpoint
	private AtomicReference<Map<String, Map<Integer, Endpoint>>> m_endpointCache = new AtomicReference<>();

	private ExecutorService m_updateTaskExecutor;

	private MetaMerger m_metaMerger = new MetaMerger();

	private BlockingQueue<Runnable> m_updateTaskQueue = new LinkedBlockingQueue<Runnable>();

	private static class MetaCache {
		private Meta m_meta;

		private String m_jsonString;

		private String m_jsonStringComplete;

		private byte[] m_compressedClientBytes;

		public MetaCache(Meta meta, String jsonString, String jsonStringComplete) {
			m_meta = meta;
			m_jsonString = jsonString;
			m_jsonStringComplete = jsonStringComplete;
			m_compressedClientBytes = ClientMeta.serialize(meta);
		}

		public Meta getMeta() {
			return m_meta;
		}

		public String getJsonString() {
			return m_jsonString;
		}

		public String getJsonStringComplete() {
			return m_jsonStringComplete;
		}

		public byte[] getCompressedClientBytes() {
			return m_compressedClientBytes;
		}

	}

	public Meta getMeta() {
		MetaCache metaCache = m_mergedMetaCache.get();
		return metaCache == null ? null : metaCache.getMeta();
	}

	public String getMetaJson(boolean needComplete) {
		MetaCache metaCache = m_mergedMetaCache.get();
		if (metaCache == null) {
			return null;
		} else {
			return needComplete ? metaCache.getJsonStringComplete() : metaCache.getJsonString();
		}
	}

	public byte[] getCompressedClientMeta() {
		MetaCache metaCache = m_mergedMetaCache.get();
		return metaCache == null ? null : metaCache.getCompressedClientBytes();
	}

	@Override
	public void initialize() throws InitializationException {
		m_updateTaskExecutor = Executors.newSingleThreadExecutor(HermesThreadFactory.create("MetaUpdater", true));
		m_updateTaskExecutor.submit(new Runnable() {

			@Override
			public void run() {
				while (true) {
					try {
						int skipped = 0;
						Runnable task = m_updateTaskQueue.take();
						if (!m_updateTaskQueue.isEmpty()) {
							LinkedList<Runnable> queuedTasks = new LinkedList<>();
							m_updateTaskQueue.drainTo(queuedTasks);
							skipped = queuedTasks.size();
							task = queuedTasks.getLast();
						}

						try {
							task.run();
						} finally {
							if (skipped > 0) {
								log.info("Skipped {} meta update tasks.", skipped);
							}
						}

					} catch (Throwable e) {
						log.error("Exeception occurred in MetaHolder's updateTaskExecutor loop", e);
					}
				}
			}
		});

		m_configedMetaServers = new AtomicReference<>();
		m_configedMetaServers.set(new HashMap<Pair<String, Integer>, Server>());

		m_idcs = new AtomicReference<>();
		m_idcs.set(new HashMap<String, Idc>());
	}

	public void setMeta(Meta meta) {
		setMetaCache(meta);
	}

	public void setMetaServers(List<Server> metaServers) {
		List<Server> servers = new ArrayList<>();
		if (metaServers != null) {
			Map<Pair<String, Integer>, Server> configedServers = m_configedMetaServers.get();
			for (Server server : metaServers) {
				Server configedServer = configedServers.get(new Pair<String, Integer>(server.getHost(), server.getPort()));
				if (configedServer != null) {
					server.setIdc(configedServer.getIdc());
					server.setEnabled(configedServer.getEnabled());
					servers.add(server);
				}
			}
		}
		m_metaServerListCache.set(servers);
	}

	public void setIdcs(Map<String, Idc> idcs) {
		m_idcs.set(idcs == null ? new HashMap<String, Idc>() : idcs);
	}

	public Map<String, Idc> getIdcs() {
		return m_idcs.get();
	}

	public void setBaseMeta(Meta baseMeta) {
		m_baseCache.set(baseMeta);
	}

	public void update(final List<Server> metaServerList) {
		update(null, metaServerList, null);
	}

	public void update(final Map<String, Map<Integer, Endpoint>> newEndpoints) {
		update(null, null, newEndpoints);
	}

	private void setMetaCache(Meta newMeta) {
		m_mergedMetaCache
		      .set(new MetaCache(newMeta, MetaUtils.filterSensitiveField(newMeta), JSON.toJSONString(newMeta)));
	}

	private void update(final Meta baseMeta, final List<Server> metaServerList,
	      final Map<String, Map<Integer, Endpoint>> newEndpoints) {
		if (baseMeta != null) {
			m_baseCache.set(baseMeta);
		}
		if (metaServerList != null) {
			setMetaServers(metaServerList);
		}
		if (newEndpoints != null) {
			m_endpointCache.set(newEndpoints);
		}

		m_updateTaskQueue.offer(new Runnable() {

			@Override
			public void run() {
				try {
					long start = System.currentTimeMillis();
					Meta newMeta = m_metaMerger.merge(m_baseCache.get(), m_metaServerListCache.get(), m_endpointCache.get(),
					      m_zkClient.getZookeeperEnsembles());
					MetaInfo metaInfo = fetchLatestMetaInfo();

					if (PlexusComponentLocator.lookup(ClusterStateHolder.class).getRole() == Role.LEADER) {
						setMetaCache(newMeta.setVersion(metaInfo.getTimestamp()));
						persistMetaInfo(metaInfo);

						traceLog.info("Update dynamic meta(id={}, version={}, meta={}).", newMeta.getId(),
						      newMeta.getVersion(), m_mergedMetaCache.get().getJsonStringComplete());
						log.info("Update dynamic meta(id={}, version={}) cost {}ms.", newMeta.getId(), newMeta.getVersion(),
						      (System.currentTimeMillis() - start));
					}
				} catch (Exception e) {
					log.error("Exception occurred while updating meta.", e);
				}
			}
		});

	}

	private MetaInfo fetchLatestMetaInfo() throws Exception {
		MetaInfo metaInfo = ZKSerializeUtils.deserialize( //
		      m_zkClient.get().getData().forPath(ZKPathUtils.getMetaInfoZkPath()), MetaInfo.class);

		long newMetaVersion = System.currentTimeMillis() / 1000L;
		// may be same due to different machine time
		if (metaInfo != null) {
			newMetaVersion = metaInfo.getTimestamp() >= newMetaVersion ? metaInfo.getTimestamp() + 1 : newMetaVersion;
		}

		if (metaInfo == null) {
			metaInfo = new MetaInfo();
		}

		metaInfo.setTimestamp(newMetaVersion);
		metaInfo.setHost(Networks.forIp().getLocalHostAddress());
		metaInfo.setPort(m_config.getMetaServerPort());
		return metaInfo;
	}

	private void persistMetaInfo(MetaInfo metaInfo) throws Exception {
		m_zkService.persist(ZKPathUtils.getMetaInfoZkPath(), ZKSerializeUtils.serialize(metaInfo));
	}

	public Map<Pair<String, Integer>, Server> getConfigedMetaServers() {
		return m_configedMetaServers.get();
	}

	public void setConfigedMetaServers(List<Server> configedMetaServers) {
		if (configedMetaServers != null) {
			Map<Pair<String, Integer>, Server> configedServers = new HashMap<Pair<String, Integer>, Server>(
			      configedMetaServers.size());
			for (Server server : configedMetaServers) {
				configedServers.put(new Pair<String, Integer>(server.getHost(), server.getPort()), server);
			}

			m_configedMetaServers.set(configedServers);
		}
	}

}
