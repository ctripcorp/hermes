package com.ctrip.hermes.consumer.engine.bootstrap.strategy;

import com.ctrip.hermes.consumer.ConsumerType;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface ConsumingStrategyRegistry {

	public ConsumingStrategy findStrategy(ConsumerType consumerType);
}
