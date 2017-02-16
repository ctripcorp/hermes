package com.ctrip.hermes.core.transport.endpoint;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.config.CoreConfig;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.meta.internal.MetaManager;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Topic;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = EndpointManager.class)
public class DefaultEndpointManager implements EndpointManager, Initializable {

	private static final Logger log = LoggerFactory.getLogger(DefaultEndpointManager.class);

	@Inject
	private MetaService m_metaService;

	@Inject
	private MetaManager m_manager;

	@Inject
	private CoreConfig m_config;

	private ConcurrentHashMap<Pair<String, Integer>, EndpointCacheValue> m_tpEndpointCache = new ConcurrentHashMap<Pair<String, Integer>, EndpointCacheValue>();

	private AtomicLong m_lastRefreshTime = new AtomicLong(0);

	private AtomicBoolean m_refreshing = new AtomicBoolean(false);

	private ExecutorService m_refreshThreadPool = Executors.newSingleThreadExecutor(HermesThreadFactory.create(
	      "EndpointCacheRefresher", true));

	@Override
	public void initialize() throws InitializationException {
		Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create("EndpointCacheHouseKeeper", true))
		      .scheduleWithFixedDelay(new Runnable() {

			      @Override
			      public void run() {
				      try {
					      long now = System.currentTimeMillis();
					      if (!m_tpEndpointCache.isEmpty()) {
						      for (Map.Entry<Pair<String, Integer>, EndpointCacheValue> entry : m_tpEndpointCache.entrySet()) {
							      if (now - entry.getValue().getAccessTime() > m_config.getEndpointCacheMillis()) {
								      m_tpEndpointCache.remove(entry.getKey());
							      }
						      }
					      }
				      } catch (Exception e) {
					      log.warn("Exception occurred in EndpointCacheCleaner", e);
				      }
			      }
		      }, 3, 3, TimeUnit.MINUTES);
	}

	@Override
	public Endpoint getEndpoint(String topic, int partition) {
		Pair<String, Integer> tp = new Pair<String, Integer>(topic, partition);
		Pair<Endpoint, Long> endpointEntryFromMeta = m_metaService.findEndpointByTopicAndPartition(topic, partition);
		EndpointCacheValue endpointEntryFromCache = m_tpEndpointCache.get(tp);

		Endpoint endpoint = null;

		if (endpointEntryFromCache != null && endpointEntryFromMeta != null) {
			if (endpointEntryFromMeta.getValue() > endpointEntryFromCache.getRefreshTime()) {
				endpoint = endpointEntryFromMeta.getKey();
				updateEndpoint(topic, partition, endpoint, endpointEntryFromMeta.getValue());
			} else {
				endpoint = endpointEntryFromCache.getEndpoint(true);
			}
		} else if (endpointEntryFromCache != null) {
			endpoint = endpointEntryFromCache.getEndpoint(true);
		} else {
			endpoint = endpointEntryFromMeta.getKey();
			updateEndpoint(topic, partition, endpoint, endpointEntryFromMeta.getValue());
		}

		return endpoint;
	}

	private void updateEndpoint(String topic, int partition, Endpoint newEndpoint, long refreshTime) {
		Pair<String, Integer> tp = new Pair<String, Integer>(topic, partition);
		EndpointCacheValue cacheValue = m_tpEndpointCache.get(tp);
		if (cacheValue == null) {
			EndpointCacheValue oldValue = m_tpEndpointCache.putIfAbsent(tp, new EndpointCacheValue(newEndpoint,
			      refreshTime));
			if (oldValue != null) {
				oldValue.setEndpoint(newEndpoint, refreshTime);
			}
		} else {
			cacheValue.setEndpoint(newEndpoint, refreshTime);
		}
	}

	@Override
	public void updateEndpoint(String topic, int partition, Endpoint newEndpoint) {
		updateEndpoint(topic, partition, newEndpoint, System.currentTimeMillis());
	}

	@Override
	public void refreshEndpoint(String topic, int partition) {
		long now = System.currentTimeMillis();
		if (now - m_lastRefreshTime.get() > m_config.getEndpointRefreshMinIntervalMillis()) {
			if (m_refreshing.compareAndSet(false, true)) {
				final Set<String> topics = new HashSet<>();
				topics.add(topic);
				for (Map.Entry<Pair<String, Integer>, EndpointCacheValue> entry : m_tpEndpointCache.entrySet()) {
					topics.add(entry.getKey().getKey());
				}

				refreshTopics(topics);
			}
		}
	}

	private void refreshTopics(final Set<String> topics) {
		m_refreshThreadPool.submit(new RefreshTopicsTask(topics));
	}

	private class RefreshTopicsTask implements Runnable {

		private Set<String> topics;

		public RefreshTopicsTask(Set<String> topics) {
			this.topics = topics;
		}

		@Override
		public void run() {
			try {
				Meta topicsMeta = m_manager.getMetaProxy().getTopicsMeta(topics);
				if (topicsMeta != null && topicsMeta.getTopics() != null) {
					updateEndpoints(topicsMeta);
				}
			} catch (Exception e) {
				// ignore it
				log.warn("Failed to refresh endpoints for topics({})", topics);
			} finally {
				m_refreshing.set(false);
				m_lastRefreshTime.set(System.currentTimeMillis());
			}
		}

		private void updateEndpoints(Meta topicsMeta) {
			long now = System.currentTimeMillis();
			for (Topic topic : topicsMeta.getTopics().values()) {
				if (topic.getPartitions() != null) {
					for (Partition partition : topic.getPartitions()) {
						if (partition.getEndpoint() != null) {
							Endpoint endpoint = topicsMeta.getEndpoints().get(partition.getEndpoint());
							if (endpoint != null) {
								m_tpEndpointCache.put(new Pair<String, Integer>(topic.getName(), partition.getId()),
								      new EndpointCacheValue(endpoint, now));
							}
						}
					}
				}
			}
		}
	}

	@Override
	public boolean containsEndpoint(Endpoint endpoint) {
		for (EndpointCacheValue endpointCacheValue : m_tpEndpointCache.values()) {
			Endpoint existingEndpoint = endpointCacheValue.getEndpoint(false);
			if (existingEndpoint != null && existingEndpoint.getId() == endpoint.getId()) {
				return true;
			}
		}
		return false;
	}

	private static class EndpointCacheValue {
		private Endpoint m_endpoint;

		private long m_refreshTime;

		private long m_accessTime;

		public EndpointCacheValue(Endpoint endpoint, long refreshTime) {
			m_endpoint = endpoint;
			m_refreshTime = refreshTime;
			m_accessTime = System.currentTimeMillis();
		}

		public synchronized Endpoint getEndpoint(boolean shouldTouch) {
			if (shouldTouch) {
				touch();
			}
			return m_endpoint;
		}

		public synchronized long getRefreshTime() {
			return m_refreshTime;
		}

		public synchronized long getAccessTime() {
			return m_accessTime;
		}

		public synchronized void setEndpoint(Endpoint endpoint, long refreshTime) {
			touch();
			m_endpoint = endpoint;
			m_refreshTime = refreshTime;
		}

		private void touch() {
			m_accessTime = System.currentTimeMillis();
		}
	}

}
