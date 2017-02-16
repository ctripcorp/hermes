package com.ctrip.hermes.metaservice.zk;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.curator.ensemble.EnsembleProvider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.HermesClientCnxnSocketNIO;
import org.apache.zookeeper.ZooKeeper;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.meta.entity.ZookeeperEnsemble;
import com.ctrip.hermes.metaservice.service.ZookeeperEnsembleService;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = ZKClient.class)
public class ZKClient implements Initializable {

	private static final Logger log = LoggerFactory.getLogger(ZKClient.class);

	private CuratorFramework m_client;

	@Inject
	private ZKConfig m_config;

	@Inject
	private ZookeeperEnsembleService m_zookeeperEnsembleService;

	private AtomicReference<List<ZookeeperEnsemble>> m_zookeeperEnsembles = new AtomicReference<>();

	private AtomicReference<ZookeeperEnsemble> m_primaryZookeeperEnsemble = new AtomicReference<>();

	@Override
	public void initialize() throws InitializationException {
		System.setProperty(ZooKeeper.ZOOKEEPER_CLIENT_CNXN_SOCKET, HermesClientCnxnSocketNIO.class.getName());

		try {
			ZookeeperEnsemble primaryEnsemble = getPrimaryEnsemble();
			m_primaryZookeeperEnsemble.set(primaryEnsemble);
			startCuratorFramework(primaryEnsemble);
		} catch (RuntimeException e) {
			throw new InitializationException("Can not init zk-client", e);

		}
	}

	private ZookeeperEnsemble getPrimaryEnsemble() {
		List<ZookeeperEnsemble> ensembles;
		try {
			ensembles = m_zookeeperEnsembleService.listEnsembles();
			if (ensembles != null && !ensembles.isEmpty()) {
				m_zookeeperEnsembles.set(ensembles);
				for (ZookeeperEnsemble ensemble : ensembles) {
					if (ensemble.isPrimary()) {
						return ensemble;
					}
				}
			} else {
				throw new RuntimeException("No zookeeper ensemble found in meta-db");
			}
		} catch (DalException e) {
			log.error("Exception occurred while listing zookeeper ensembles", e);
			throw new RuntimeException("Exception occurred while listing zookeeper ensembles", e);
		}

		throw new RuntimeException("Primary zookeeper ensemble not found in meta-db");
	}

	private void startCuratorFramework(ZookeeperEnsemble primaryEnsemble) throws InitializationException {
		Builder builder = CuratorFrameworkFactory.builder();

		builder.connectionTimeoutMs(m_config.getZkConnectionTimeoutMillis());
		builder.maxCloseWaitMs(m_config.getZkCloseWaitMillis());
		builder.namespace(m_config.getZkNamespace());
		builder.retryPolicy(new RetryNTimes(m_config.getZkRetries(), m_config.getSleepMsBetweenRetries()));
		builder.sessionTimeoutMs(m_config.getZkSessionTimeoutMillis());
		builder.threadFactory(HermesThreadFactory.create("MetaService-Zk", true));
		builder.ensembleProvider(new EnsembleProvider() {

			@Override
			public void start() throws Exception {
			}

			@Override
			public String getConnectionString() {
				return m_primaryZookeeperEnsemble.get().getConnectionString();
			}

			@Override
			public void close() throws IOException {
			}
		});

		m_client = builder.build();
		m_client.start();
		try {
			m_client.blockUntilConnected();
			log.info("Conneted to zookeeper({}).", JSON.toJSONString(primaryEnsemble));
		} catch (InterruptedException e) {
			throw new InitializationException(e.getMessage(), e);
		}
	}

	public boolean pauseAndSwitchPrimaryEnsemble() throws Exception {
		ZookeeperEnsemble newPrimaryEnsemble = getPrimaryEnsemble();
		synchronized (this) {
			ZookeeperEnsemble oldPrimaryEnsemble = m_primaryZookeeperEnsemble.get();
			if (oldPrimaryEnsemble.getConnectionString().equalsIgnoreCase(newPrimaryEnsemble.getConnectionString())) {
				log.info("Zookeeper ensemble's connection string is unchanged({}).", JSON.toJSONString(oldPrimaryEnsemble));
				return false;
			} else {
				m_primaryZookeeperEnsemble.set(newPrimaryEnsemble);

				TimeUnit.MILLISECONDS.sleep(Math.max(m_config.getZkSessionTimeoutMillis(),
				      m_config.getZkConnectionTimeoutMillis()) + 1000);

				m_client.getZookeeperClient().getZooKeeper();

				log.info("Zookeeper ensemble's connection string changed from {} to {}.",
				      JSON.toJSONString(oldPrimaryEnsemble), JSON.toJSONString(newPrimaryEnsemble));
				HermesClientCnxnSocketNIO.pause();
				log.info("Zookeeper client paused.");
				return true;
			}
		}
	}

	public void resume() {
		HermesClientCnxnSocketNIO.resume();
		log.info("Zookeeper client resume.");
	}

	public boolean isPaused() {
		return HermesClientCnxnSocketNIO.isPaused();
	}

	public CuratorFramework get() {
		return m_client;
	}

	public List<ZookeeperEnsemble> getZookeeperEnsembles() {
		return m_zookeeperEnsembles.get();
	}

}
