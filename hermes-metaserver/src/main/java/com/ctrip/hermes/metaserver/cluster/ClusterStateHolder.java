package com.ctrip.hermes.metaserver.cluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatch.CloseMode;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.locks.LockInternals;
import org.apache.curator.framework.recipes.locks.LockInternalsSorter;
import org.apache.curator.framework.recipes.locks.StandardLockInternalsDriver;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.net.Networks;

import com.ctrip.hermes.core.bo.HostPort;
import com.ctrip.hermes.core.constants.CatConstants;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.metaserver.broker.BrokerLeaseHolder;
import com.ctrip.hermes.metaserver.config.MetaServerConfig;
import com.ctrip.hermes.metaserver.consumer.ConsumerLeaseHolder;
import com.ctrip.hermes.metaserver.event.Event;
import com.ctrip.hermes.metaserver.event.EventBus;
import com.ctrip.hermes.metaserver.event.EventType;
import com.ctrip.hermes.metaserver.event.Guard;
import com.ctrip.hermes.metaservice.zk.ZKClient;
import com.ctrip.hermes.metaservice.zk.ZKPathUtils;
import com.ctrip.hermes.metaservice.zk.ZKSerializeUtils;
import com.dianping.cat.Cat;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = ClusterStateHolder.class)
public class ClusterStateHolder implements Initializable {

	private static final Logger log = LoggerFactory.getLogger(ClusterStateHolder.class);

	@Inject
	private MetaServerConfig m_config;

	@Inject
	private ZKClient m_client;

	@Inject
	private EventBus m_eventBus;

	private AtomicReference<HostPort> m_leader = new AtomicReference<>(null);

	private volatile LeaderLatch m_leaderLatch = null;

	private AtomicBoolean m_closed = new AtomicBoolean(false);

	@Inject
	private ConsumerLeaseHolder m_consumerLeaseHolder;

	@Inject
	private BrokerLeaseHolder m_brokerLeaseHolder;

	@Inject
	private Guard m_guard;

	private ReentrantReadWriteLock m_roleLock = new ReentrantReadWriteLock(true);

	private ExecutorService m_roleChangeExecutor;

	private volatile Role m_role = null;

	private AtomicBoolean m_connected = new AtomicBoolean(true);

	private PathChildrenCache m_leaderLatchPathChildrenCache;

	private volatile boolean m_leaseAssigning = true;

	public void becomeLeader() {
		m_roleLock.writeLock().lock();
		try {
			log.info("Become Leader!!!");
			Cat.logEvent(CatConstants.TYPE_ROLE_CHANGED, "Leader");
			m_role = Role.LEADER;
			long newVersion = m_guard.upgradeVersion();
			m_leader.set(new HostPort(Networks.forIp().getLocalHostAddress(), m_config.getMetaServerPort()));
			m_eventBus.pubEvent(new Event(EventType.LEADER_INIT, newVersion, null));
		} finally {
			m_roleLock.writeLock().unlock();
		}
	}

	public void becomeFollower() {
		m_roleLock.writeLock().lock();
		try {
			log.info("Become Follower!!!");
			Cat.logEvent(CatConstants.TYPE_ROLE_CHANGED, "Follower");
			m_role = Role.FOLLOWER;
			long newVersion = m_guard.upgradeVersion();
			startLeaderLatch();
			m_eventBus.pubEvent(new Event(EventType.FOLLOWER_INIT, newVersion, null));
		} finally {
			m_roleLock.writeLock().unlock();
		}
	}

	private void startLeaderLatch() {
		if (m_leaderLatch == null) {
			m_leaderLatch = new LeaderLatch(m_client.get(), m_config.getMetaServerLeaderElectionZkPath(),
			      m_config.getMetaServerName());

			m_leaderLatch.addListener(new LeaderLatchListener() {

				@Override
				public void notLeader() {
					if (m_connected.get()) {
						becomeFollower();
					}
				}

				@Override
				public void isLeader() {
					becomeLeader();
				}
			}, m_roleChangeExecutor);

			try {
				m_leaderLatch.start();
				log.info("LeaderLatch started");
			} catch (Exception e) {
				log.error("Failed to start LeaderLatch!!!", e);
			}
		}
	}

	public void becomeObserver() {
		m_roleLock.writeLock().lock();
		try {
			log.info("Become Observer!!!");
			Cat.logEvent(CatConstants.TYPE_ROLE_CHANGED, "Observer");
			m_role = Role.OBSERVER;
			long newVersion = m_guard.upgradeVersion();
			closeLeaderLatch();
			m_eventBus.pubEvent(new Event(EventType.OBSERVER_INIT, newVersion, null));
		} finally {
			m_roleLock.writeLock().unlock();
		}
	}

	private void closeLeaderLatch() {
		if (m_leaderLatch != null) {
			try {
				m_leaderLatch.close(CloseMode.SILENT);
				log.info("LeaderLatch closed!!!");
			} catch (IOException e) {
				log.error("Exception occurred while closing leaderLatch.", e);
			} finally {
				m_leaderLatch = null;
			}
		}
	}

	public Role getRole() {
		m_roleLock.readLock().lock();
		try {
			return m_role;
		} finally {
			m_roleLock.readLock().unlock();
		}
	}

	public void start() throws Exception {
		becomeObserver();
	}

	public void close() throws Exception {
		if (m_closed.compareAndSet(false, true)) {
			closeLeaderLatch();
		}
	}

	public HostPort getLeader() {
		return m_leader.get();
	}

	private void updateLeaderInfo() {
		List<ChildData> children = m_leaderLatchPathChildrenCache.getCurrentData();

		if (children != null && !children.isEmpty()) {
			List<String> childrenNames = new ArrayList<>(children.size());
			Map<String, byte[]> nameDataMapping = new HashMap<>(children.size());
			for (ChildData child : children) {
				if (child.getData() != null && child.getData().length > 0) {
					String name = ZKPathUtils.lastSegment(child.getPath());
					childrenNames.add(name);
					nameDataMapping.put(name, child.getData());
				}
			}

			List<String> sortedChildren = LockInternals.getSortedChildren("latch-", new LockInternalsSorter() {

				@Override
				public String fixForSorting(String str, String lockName) {
					return StandardLockInternalsDriver.standardFixForSorting(str, lockName);
				}
			}, childrenNames);

			m_leader.set(ZKSerializeUtils.deserialize(nameDataMapping.get(sortedChildren.get(0)), HostPort.class));
		}
	}

	public boolean isConnected() {
		return m_connected.get();
	}

	@Override
	public void initialize() throws InitializationException {
		m_roleChangeExecutor = Executors.newSingleThreadExecutor(HermesThreadFactory.create("ClusterRoleChangeExecutor",
		      true));

		m_eventBus.start(this);

		addConnectionStateListener();
		startLeaderLatchPathChildrenCache();
	}

	private void addConnectionStateListener() {
		m_client.get().getConnectionStateListenable().addListener(new ConnectionStateListener() {

			@Override
			public void stateChanged(CuratorFramework client, ConnectionState newState) {
				switch (newState) {
				case LOST:
					if (m_connected.get()) {
						m_connected.set(false);
						m_guard.upgradeVersion();
						log.info("Disconnected from zk(state:{})", newState);
					}
					break;
				case RECONNECTED:
				case CONNECTED:
					if (!m_connected.get()) {
						m_connected.set(true);
						becomeObserver();
						log.info("Reconnected to zk(state:{})", newState);
					}
					break;
				default:
					break;
				}
			}
		}, m_roleChangeExecutor);
	}

	private void startLeaderLatchPathChildrenCache() throws InitializationException {
		try {
			m_leaderLatchPathChildrenCache = new PathChildrenCache(m_client.get(), ZKPathUtils.getMetaServersZkPath(),
			      true);

			m_leaderLatchPathChildrenCache.getListenable().addListener(
			      new PathChildrenCacheListener() {

				      @Override
				      public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
					      if (event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED
					            || event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED
					            || event.getType() == PathChildrenCacheEvent.Type.CHILD_UPDATED) {
						      updateLeaderInfo();
					      }
				      }
			      },
			      Executors.newSingleThreadExecutor(HermesThreadFactory.create(
			            "LeaderLatchPathChildrenCacheListenerExecutor", true)));

			m_leaderLatchPathChildrenCache.start(StartMode.BUILD_INITIAL_CACHE);
		} catch (Exception e) {
			throw new InitializationException("Init metaServer leaderLatch parent pathChildrenCache failed.", e);
		}

		updateLeaderInfo();
	}

	// for test only
	public void setRole(Role role) {
		m_role = role;
	}

	public boolean isLeaseAssigning() {
		return m_leaseAssigning;
	}

	public void setLeaseAssigning(boolean m_leaseAssigning) {
		this.m_leaseAssigning = m_leaseAssigning;
	}
}
