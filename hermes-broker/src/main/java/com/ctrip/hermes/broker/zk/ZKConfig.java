package com.ctrip.hermes.broker.zk;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.env.ClientEnvironment;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = ZKConfig.class)
public class ZKConfig {

	@Inject
	ClientEnvironment env;

	public int getZkConnectionTimeoutMillis() {
		return 3000;
	}

	public int getZkCloseWaitMillis() {
		return 1000;
	}

	public String getZkNamespace() {
		return "hermes";
	}

	public int getSleepMsBetweenRetries() {
		return 100;
	}

	public int getZkRetries() {
		return 3;
	}

	public int getZkSessionTimeoutMillis() {
		return 10 * 1000;
	}
}
