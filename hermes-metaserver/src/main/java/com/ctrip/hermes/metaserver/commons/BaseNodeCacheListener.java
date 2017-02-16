package com.ctrip.hermes.metaserver.commons;

import org.apache.curator.framework.listen.ListenerContainer;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.metaserver.cluster.ClusterStateHolder;
import com.ctrip.hermes.metaserver.event.EventBus;
import com.ctrip.hermes.metaserver.event.VersionGuardedTask;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public abstract class BaseNodeCacheListener implements NodeCacheListener {

	private static final Logger log = LoggerFactory.getLogger(BaseNodeCacheListener.class);

	protected EventBus m_eventBus;

	protected long m_version;

	protected ClusterStateHolder m_clusterStateHolder;

	private ListenerContainer<NodeCacheListener> m_listenerContainer;

	protected BaseNodeCacheListener(long version, ListenerContainer<NodeCacheListener> listenerContainer) {
		m_version = version;
		m_eventBus = PlexusComponentLocator.lookup(EventBus.class);
		m_clusterStateHolder = PlexusComponentLocator.lookup(ClusterStateHolder.class);
		m_listenerContainer = listenerContainer;
	}

	@Override
	public void nodeChanged() throws Exception {
		new VersionGuardedTask(m_version) {

			@Override
			public String name() {
				return getName();
			}

			@Override
			protected void doRun() {
				processNodeChanged();
			}

			@Override
			protected void onGuardNotPass() {
				removeListener();
			}
		}.run();

	}

	protected void removeListener() {
		log.info("NodeCacheListener invalidate, will remove from nodeCache(name:{}, version:{})", getName(), m_version);
		m_listenerContainer.removeListener(this);
	}

	protected abstract void processNodeChanged();

	protected abstract String getName();

}
