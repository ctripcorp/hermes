package com.ctrip.hermes.consumer.engine.bootstrap;

import com.ctrip.hermes.meta.entity.Topic;



/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface ConsumerBootstrapManager {

	public ConsumerBootstrap findConsumerBootStrap(Topic topic);

}
