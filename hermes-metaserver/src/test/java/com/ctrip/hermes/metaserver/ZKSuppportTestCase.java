package com.ctrip.hermes.metaserver;

import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Properties;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.EnsurePath;
import org.apache.curator.utils.PathUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.Mock;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.env.ClientEnvironment;
import com.ctrip.hermes.metaservice.zk.ZKConfig;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public abstract class ZKSuppportTestCase extends ComponentTestCase {
	private static TestingServer m_zkServer;

	private static final int ZK_PORT = 2222;

	protected CuratorFramework m_curator;

	@Mock
	protected ClientEnvironment m_env;

	@AfterClass
	public static void afterClass() throws Exception {
		stopZkServer();
	}

	@BeforeClass
	public static void beforeClass() throws Exception {
		startZkServer();
	}

	private static void startZkServer() throws Exception {
		m_zkServer = new TestingServer(ZK_PORT);
	}

	private static void stopZkServer() throws Exception {
		if (m_zkServer != null) {
			m_zkServer.close();
		}
	}

	private void clearZk() throws Exception {
		deleteChildren("/", true);
	}

	protected void configureCurator() throws Exception {

		Builder builder = CuratorFrameworkFactory.builder();

		builder.connectionTimeoutMs(50);
		builder.connectString(getZkConnectionString());
		builder.maxCloseWaitMs(50);
		builder.namespace("hermes");
		builder.retryPolicy(new ExponentialBackoffRetry(5, 3));
		builder.sessionTimeoutMs(50);

		m_curator = builder.build();
		m_curator.start();
		try {
			m_curator.blockUntilConnected();
		} catch (InterruptedException e) {
			throw new InitializationException(e.getMessage(), e);
		}

	}

	protected void deleteChildren(String path, boolean deleteSelf) throws Exception {
		PathUtils.validatePath(path);

		CuratorFramework client = m_curator;
		Stat stat = client.checkExists().forPath(path);
		if (stat != null) {
			List<String> children = client.getChildren().forPath(path);
			for (String child : children) {
				String fullPath = ZKPaths.makePath(path, child);
				deleteChildren(fullPath, true);
			}

			if (deleteSelf) {
				try {
					client.delete().forPath(path);
				} catch (KeeperException.NotEmptyException e) {
					// someone has created a new child since we checked ... delete again.
					deleteChildren(path, true);
				} catch (KeeperException.NoNodeException e) {
					// ignore... someone else has deleted the node it since we checked
				}

			}
		}
	}

	protected void ensurePath(String path) throws Exception {
		EnsurePath ensurePath = m_curator.newNamespaceAwareEnsurePath(path);
		ensurePath.ensure(m_curator.getZookeeperClient());
	}

	private String getZkConnectionString() {
		return "127.0.0.1:" + ZK_PORT;
	}

	protected abstract void initZkData() throws Exception;

	@Before
	@Override
	public void setUp() throws Exception {
		configureCurator();
		initZkData();

		super.setUp();

		Properties globalConf = new Properties();
		globalConf.put("meta.zk.connectionString", getZkConnectionString());
		when(m_env.getGlobalConfig()).thenReturn(globalConf);
		lookup(ZKConfig.class).setEnv(m_env);

		doSetUp();
	}

	protected void doSetUp() throws Exception {

	}

	@After
	@Override
	public void tearDown() throws Exception {
		super.tearDown();
		clearZk();
	}
}
