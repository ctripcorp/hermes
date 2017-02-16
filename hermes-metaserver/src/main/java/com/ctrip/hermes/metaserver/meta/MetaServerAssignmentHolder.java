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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.alibaba.fastjson.TypeReference;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Server;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaserver.cluster.ClusterStateHolder;
import com.ctrip.hermes.metaserver.cluster.Role;
import com.ctrip.hermes.metaserver.commons.Assignment;
import com.ctrip.hermes.metaserver.commons.ClientContext;
import com.ctrip.hermes.metaserver.log.LoggerConstants;
import com.ctrip.hermes.metaservice.service.ZookeeperService;
import com.ctrip.hermes.metaservice.zk.ZKClient;
import com.ctrip.hermes.metaservice.zk.ZKPathUtils;
import com.ctrip.hermes.metaservice.zk.ZKSerializeUtils;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = MetaServerAssignmentHolder.class)
public class MetaServerAssignmentHolder implements Initializable {

	private static final Logger log = LoggerFactory.getLogger(MetaServerAssignmentHolder.class);

	private static final Logger traceLog = LoggerFactory.getLogger(LoggerConstants.TRACE);

	@Inject
	private ZookeeperService m_zkService;

	@Inject
	private ZKClient m_zkClient;

	@Inject
	private MetaServerAssigningStrategy m_metaServerAssigningStrategy;

	private AtomicReference<List<Server>> m_metaServersCache = new AtomicReference<>();

	private AtomicReference<List<Topic>> m_topicsCache = new AtomicReference<>();

	private PathChildrenCache m_pathChildrenCache;

	private Cache<String, Map<String, ClientContext>> m_topiAssignmentCache;

	private BlockingQueue<Runnable> m_persistZKTaskQueue = new LinkedBlockingQueue<>();

	@Override
	public void initialize() throws InitializationException {
		m_topiAssignmentCache = CacheBuilder.newBuilder().maximumSize(2000).build();
		initPathChildrenCache();

		startPersistZkThread();
	}

	private void startPersistZkThread() {
		ExecutorService persistTaskExecutor = Executors.newSingleThreadExecutor(HermesThreadFactory.create(
		      "MetaServerAssignmentZKPersistThread", true));
		persistTaskExecutor.submit(new Runnable() {

			@Override
			public void run() {
				while (true) {
					try {
						int skipped = 0;
						Runnable task = m_persistZKTaskQueue.take();
						if (!m_persistZKTaskQueue.isEmpty()) {
							LinkedList<Runnable> queuedTasks = new LinkedList<>();
							m_persistZKTaskQueue.drainTo(queuedTasks);
							skipped = queuedTasks.size();
							task = queuedTasks.getLast();
						}

						try {
							task.run();
						} finally {
							if (skipped > 0) {
								log.info("Skipped {} metaServerAssignment persistence tasks.", skipped);
							}
						}

					} catch (Throwable e) {
						log.error("Exeception occurred in MetaServerAssignmentZKPersistThread loop", e);
					}
				}
			}
		});
	}

	private void initPathChildrenCache() throws InitializationException {
		m_pathChildrenCache = new PathChildrenCache(m_zkClient.get(), ZKPathUtils.getMetaServerAssignmentRootZkPath(),
		      true);

		m_pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {

			@Override
			public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
				if (event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED
				      || event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED
				      || event.getType() == PathChildrenCacheEvent.Type.CHILD_UPDATED) {
					String topic = ZKPathUtils.lastSegment(event.getData().getPath());
					m_topiAssignmentCache.invalidate(topic);
				}
			}
		});

		try {
			m_pathChildrenCache.start(StartMode.BUILD_INITIAL_CACHE);
		} catch (Exception e) {
			throw new InitializationException("Init metaServerAssignmentHolder failed.", e);
		}
	}

	public Map<String, ClientContext> getAssignment(String topic) {
		Map<String, ClientContext> assignment = m_topiAssignmentCache.getIfPresent(topic);
		if (assignment == null) {
			assignment = getAssignmentWithoutCache(topic);
			m_topiAssignmentCache.put(topic, assignment);
		}

		return assignment;
	}

	private Map<String, ClientContext> getAssignmentWithoutCache(String topic) {
		Map<String, ClientContext> assignment = null;
		ChildData node = m_pathChildrenCache.getCurrentData(ZKPathUtils.getMetaServerAssignmentZkPath(topic));
		if (node != null) {
			assignment = ZKSerializeUtils.deserialize(node.getData(), new TypeReference<Map<String, ClientContext>>() {
			}.getType());
		}
		return assignment == null ? new HashMap<String, ClientContext>() : assignment;
	}

	public Assignment<String> getAssignments() {
		Assignment<String> assignment = new Assignment<>();
		List<ChildData> topicNodes = m_pathChildrenCache.getCurrentData();

		if (topicNodes != null) {
			for (ChildData topicNode : topicNodes) {
				String topic = ZKPathUtils.lastSegment(topicNode.getPath());
				assignment.addAssignment(topic, getAssignmentWithoutCache(topic));
			}
		}

		return assignment;
	}

	public void reassign(List<Server> metaServers, Map<Pair<String, Integer>, Server> configedMetaServers,
	      List<Topic> topics) {
		if (metaServers != null) {
			setMetaServersCache(metaServers, configedMetaServers);
		}

		if (topics != null) {
			m_topicsCache.set(topics);
		}
		Assignment<String> newAssignments = m_metaServerAssigningStrategy.assign(m_metaServersCache.get(),
		      m_topicsCache.get(), getAssignments());
		updateAssignments(newAssignments);

		if (traceLog.isInfoEnabled()) {
			traceLog.info("Meta server assignment changed.(new assignment={})", newAssignments.toString());
		}
	}

	private void setMetaServersCache(List<Server> metaServers, Map<Pair<String, Integer>, Server> configedMetaServers) {
		if (metaServers != null && configedMetaServers != null) {
			List<Server> mergedMetaServers = new ArrayList<>(metaServers.size());
			for (Server metaServer : metaServers) {
				Server configedMetaServer = configedMetaServers.get(new Pair<String, Integer>(metaServer.getHost(),
				      metaServer.getPort()));
				if (configedMetaServer != null) {
					Server tmpServer = new Server();
					tmpServer.setEnabled(configedMetaServer.getEnabled());
					tmpServer.setHost(configedMetaServer.getHost());
					tmpServer.setId(metaServer.getId());
					tmpServer.setIdc(configedMetaServer.getIdc());
					tmpServer.setPort(configedMetaServer.getPort());
					mergedMetaServers.add(tmpServer);
				}
			}

			m_metaServersCache.set(mergedMetaServers);
		}
	}

	private void updateAssignments(final Assignment<String> newAssignments) {
		if (newAssignments != null) {
			m_persistZKTaskQueue.offer(new Runnable() {
				@Override
				public void run() {
					if (PlexusComponentLocator.lookup(ClusterStateHolder.class).getRole() == Role.LEADER) {
						persistToZk(newAssignments, getAssignments());
					}
				}
			});
		}
	}

	private void persistToZk(Assignment<String> newAssignments, Assignment<String> originAssignments) {
		if (newAssignments != null) {
			Map<String, byte[]> zkPathAndDatas = new HashMap<>();
			long start = System.currentTimeMillis();
			try {
				for (Map.Entry<String, Map<String, ClientContext>> entry : newAssignments.getAssignments().entrySet()) {
					String topic = entry.getKey();
					Map<String, ClientContext> metaServers = entry.getValue();

					if (originAssignments != null) {
						Map<String, ClientContext> originMetaServers = originAssignments.getAssignment(topic);
						if (originMetaServers == null || originMetaServers.isEmpty()) {
							putToPathDataPair(zkPathAndDatas, topic, metaServers);
						} else {
							if (!originMetaServers.keySet().equals(metaServers.keySet())) {
								putToPathDataPair(zkPathAndDatas, topic, metaServers);
							}
						}
					} else {
						putToPathDataPair(zkPathAndDatas, topic, metaServers);
					}
				}

				try {
					m_zkService.persistBulk(zkPathAndDatas);
				} catch (Exception e) {
					log.error("Failed to persisit meta server assignments to zk.", e);
				}
			} finally {
				log.info("Persist {} metaServerAssignments cost {}ms.", zkPathAndDatas.size(),
				      (System.currentTimeMillis() - start));
			}
		}
	}

	private void putToPathDataPair(Map<String, byte[]> zkPathAndDatas, String topic,
	      Map<String, ClientContext> metaServers) {
		String path = ZKPathUtils.getMetaServerAssignmentZkPath(topic);
		byte[] data = ZKSerializeUtils.serialize(metaServers);
		zkPathAndDatas.put(path, data);
	}

}
