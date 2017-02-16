package com.ctrip.hermes.broker;

import java.util.Random;

import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.test.jetty.JettyServer;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class StartBroker extends JettyServer {
	private static final Logger log = LoggerFactory.getLogger(StartBroker.class);

	private TestingServer m_zkServer;

	private Random m_random = new Random();

	@Before
	public void before() throws Exception {
		String zkMode = System.getProperty("zkMode");
		if (!"real".equalsIgnoreCase(zkMode)) {
			try {
				m_zkServer = new TestingServer(2181);
			} catch (Exception e) {
				System.out.println("Start zk fake server failed, may be already started.");
			}
		}

		startServer();
	}

	@After
	public void after() throws Exception {
		if (m_zkServer != null) {
			m_zkServer.close();
		}
		super.stopServer();
	}

	@Test
	public void test() throws Exception {
		System.in.read();
	}

	@Override
	protected String getContextPath() {
		return "/";
	}

	@Override
	protected int getServerPort() {
		int port = getRandomPort();
		log.info("Jetty started at port {}", port);
		return port;
	}

	private int getRandomPort() {
		int port = 0;
		while (port < 10000) {
			port = m_random.nextInt(30000);
		}

		return port;
	}

}
