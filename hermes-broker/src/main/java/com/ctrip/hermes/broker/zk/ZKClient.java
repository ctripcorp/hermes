package com.ctrip.hermes.broker.zk;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

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
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.meta.entity.ZookeeperEnsemble;

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
	private MetaService m_metaService;

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

		Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create("Broker-ZKEnsemble-Checker", true))
		      .scheduleWithFixedDelay(new Runnable() {

			      @Override
			      public void run() {
				      try {
					      ZookeeperEnsemble newPrimaryEnsemble = getPrimaryEnsemble();
					      ZookeeperEnsemble oldPrimaryEnsemble = m_primaryZookeeperEnsemble.get();
					      if (!oldPrimaryEnsemble.getConnectionString().equalsIgnoreCase(
					            newPrimaryEnsemble.getConnectionString())) {
						      log.info("Zookeeper ensemble's connection string changed from {} to {}.",
						            JSON.toJSONString(oldPrimaryEnsemble), JSON.toJSONString(newPrimaryEnsemble));
						      m_primaryZookeeperEnsemble.set(newPrimaryEnsemble);
						      switchEnsemble();
					      }
				      } catch (Exception e) {
					      log.error("Exception occurred while doing zk ensemble check task", e);
				      }
			      }

		      }, 10, 10, TimeUnit.SECONDS);
	}

	private void switchEnsemble() {
		HermesClientCnxnSocketNIO.pause();
		log.info("Zookeeper client paused.");

		LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(Math.max(2 * m_config.getZkSessionTimeoutMillis(),
		      m_config.getZkConnectionTimeoutMillis() + 1000)));
		try {
			m_client.getZookeeperClient().getZooKeeper();
		} catch (Exception e) {
			log.error("Exception occurred in getZooKeeper.", e);
		}

		HermesClientCnxnSocketNIO.resume();
		log.info("Zookeeper client resume.");
	}

	private void startCuratorFramework(ZookeeperEnsemble primaryEnsemble) throws InitializationException {
		Builder builder = CuratorFrameworkFactory.builder();

		builder.connectionTimeoutMs(m_config.getZkConnectionTimeoutMillis());
		builder.maxCloseWaitMs(m_config.getZkCloseWaitMillis());
		builder.namespace(m_config.getZkNamespace());
		builder.retryPolicy(new RetryNTimes(m_config.getZkRetries(), m_config.getSleepMsBetweenRetries()));
		builder.sessionTimeoutMs(m_config.getZkSessionTimeoutMillis());
		builder.threadFactory(HermesThreadFactory.create("Broker-Zk", true));
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

	private ZookeeperEnsemble getPrimaryEnsemble() {
		List<ZookeeperEnsemble> ensembles = m_metaService.listAllZookeeperEnsemble();

		if (ensembles != null && !ensembles.isEmpty()) {
			for (ZookeeperEnsemble ensemble : ensembles) {
				if (ensemble.isPrimary()) {
					return ensemble;
				}
			}
		} else {
			throw new RuntimeException("No zookeeper ensemble found in meta");
		}

		throw new RuntimeException("Primary zookeeper ensemble not found in meta");
	}

	public CuratorFramework get() {
		return m_client;
	}
}
