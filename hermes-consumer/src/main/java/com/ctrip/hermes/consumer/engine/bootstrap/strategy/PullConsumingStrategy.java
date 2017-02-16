package com.ctrip.hermes.consumer.engine.bootstrap.strategy;

import com.ctrip.hermes.consumer.engine.ConsumerContext;

public class PullConsumingStrategy extends BaseConsumingStrategy {

	@Override
	protected ConsumerTask getConsumerTask(ConsumerContext context, int partitionId, int localCacheSize,
	      int maxAckHolderSize) {
		return new PullConsumingStrategyConsumerTask(context, partitionId, localCacheSize, maxAckHolderSize);
	}

}
