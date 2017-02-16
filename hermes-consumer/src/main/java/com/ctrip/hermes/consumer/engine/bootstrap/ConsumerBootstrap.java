package com.ctrip.hermes.consumer.engine.bootstrap;

import com.ctrip.hermes.consumer.engine.ConsumerContext;
import com.ctrip.hermes.consumer.engine.SubscribeHandle;

public interface ConsumerBootstrap {

	public SubscribeHandle start(ConsumerContext consumerContext);

	public void stop(ConsumerContext consumerContext);

}
