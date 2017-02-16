package com.ctrip.hermes.consumer.engine.bootstrap;



/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface ConsumerBootstrapRegistry {
	public void registerBootstrap(String endpointType, ConsumerBootstrap consumerBootstrap);

	public ConsumerBootstrap findConsumerBootstrap(String endpointType);
}
