package com.ctrip.hermes.broker.bootstrap;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface BrokerBootstrap {
	public void start() throws Exception;

	public void stop() throws Exception;
}
