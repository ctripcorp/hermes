package com.ctrip.hermes.consumer.engine.bootstrap.strategy;

import com.ctrip.hermes.consumer.engine.ConsumerContext;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class StrictlyOrderedConsumingStrategy extends BaseConsumingStrategy {

	@Override
	protected ConsumerTask getConsumerTask(ConsumerContext context, int partitionId, int localCacheSize, int maxAckHolderSize) {
		return new StrictlyOrderedConsumingStrategyConsumerTask(context, partitionId, localCacheSize, maxAckHolderSize);
	}

}
