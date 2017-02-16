package com.ctrip.hermes.metaserver;

import org.apache.curator.test.TestingServer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.webapp.WebAppContext;
import org.mortbay.servlet.GzipFilter;
import org.unidal.test.jetty.JettyServer;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.metaserver.config.MetaServerConfig;
import com.ctrip.hermes.metaservice.service.MetaService;
import com.ctrip.hermes.metaservice.service.ZookeeperService;
import com.ctrip.hermes.metaservice.zk.ZKPathUtils;
import com.dianping.cat.Cat;

@RunWith(JUnit4.class)
public class StartMetaServer extends JettyServer {
	private TestingServer m_zkServer = null;

	public static void main(String[] args) throws Exception {
		StartMetaServer server = new StartMetaServer();

		server.startServer();
		server.startWebapp();
		server.stopServer();
	}

	@Override
	public void startServer() throws Exception {
		String zkMode = System.getProperty("zkMode");
		if (!"real".equalsIgnoreCase(zkMode)) {
			try {
				m_zkServer = new TestingServer(2181);
				System.out.println("Starting zk with fake mode, connection string is " + m_zkServer.getConnectString());
			} catch (Exception e) {
				System.out.println("Start zk fake server failed, may be already started.");
			}
			setupZKNodes();
		}

		super.startServer();
	}

	private void setupZKNodes() throws Exception {
		ZookeeperService zkService = PlexusComponentLocator.lookup(ZookeeperService.class);
		MetaService metaService = PlexusComponentLocator.lookup(MetaService.class);

		Meta meta = metaService.getMetaEntity();
		zkService.updateZkBaseMetaVersion(meta.getVersion());

		zkService.ensurePath(ZKPathUtils.getMetaInfoZkPath());
		zkService.ensurePath(ZKPathUtils.getMetaServerAssignmentRootZkPath());
	}

	@Override
	public void stopServer() throws Exception {
		super.stopServer();
		if (m_zkServer != null) {
			m_zkServer.close();
		}
	}

	@Before
	public void before() throws Exception {
		System.setProperty("devMode", "false");
		startServer();
	}

	@Override
	protected String getContextPath() {
		return "/";
	}

	@Override
	protected int getServerPort() {
		return PlexusComponentLocator.lookup(MetaServerConfig.class).getMetaServerPort();
	}

	@Override
	protected void postConfigure(WebAppContext context) {
		context.addFilter(GzipFilter.class, "/*", Handler.ALL);
	}

	@Test
	public void startWebapp() throws Exception {
		// open the page in the default browser
		waitForAnyKey();
	}
}
