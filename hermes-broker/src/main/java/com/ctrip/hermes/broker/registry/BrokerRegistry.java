package com.ctrip.hermes.broker.registry;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface BrokerRegistry {
	public void start() throws Exception;

	public void stop() throws Exception;
}
