package com.ctrip.hermes.kafka.server;

import java.io.IOException;

import org.I0Itec.zkclient.ZkConnection;
import org.apache.curator.test.TestingServer;

public class MockZookeeper {

	private TestingServer zkTestServer;

	public MockZookeeper() {
		try {
			zkTestServer = new TestingServer(8121, false);
			start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void start() {
		try {
			zkTestServer.start();
			System.out.format("embedded zookeeper is up %s%n", zkTestServer.getConnectString());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void stop() {
		if (zkTestServer != null) {
			try {
				zkTestServer.stop();
				System.out.format("embedded zookeeper is down %s%n", zkTestServer.getConnectString());
			} catch (IOException e) {
				e.printStackTrace();
			}

		}
	}

	public ZkConnection getConnection() {
		return new ZkConnection(zkTestServer.getConnectString());
	}
}
