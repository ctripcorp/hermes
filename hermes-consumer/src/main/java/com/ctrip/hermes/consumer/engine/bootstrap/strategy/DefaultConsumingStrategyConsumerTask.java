package com.ctrip.hermes.consumer.engine.bootstrap.strategy;

import com.ctrip.hermes.consumer.engine.ConsumerContext;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class DefaultConsumingStrategyConsumerTask extends BaseConsumerTask {

	public DefaultConsumingStrategyConsumerTask(ConsumerContext context, int partitionId, int cacheSize, int maxAckHolderSize) {
		super(context, partitionId, cacheSize, maxAckHolderSize);
	}

}
