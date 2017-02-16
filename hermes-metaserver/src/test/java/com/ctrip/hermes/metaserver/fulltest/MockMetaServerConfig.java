package com.ctrip.hermes.metaserver.fulltest;

import com.ctrip.hermes.metaserver.config.MetaServerConfig;

public class MockMetaServerConfig extends MetaServerConfig {

	public static long BROKER_LEASE_TIMEOUT= 3000;

	int port;
	@Override
	public int getMetaServerPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	@Override
	public long getBrokerLeaseTimeMillis() {
		return BROKER_LEASE_TIMEOUT;
	}

	public static void  setBrokerLeaseTimeout(long timeout) {
		BROKER_LEASE_TIMEOUT = timeout;
	}
}
