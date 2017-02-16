package com.ctrip.hermes.consumer.engine.bootstrap;

import java.util.List;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.consumer.engine.CompositeSubscribeHandle;
import com.ctrip.hermes.consumer.engine.ConsumerContext;
import com.ctrip.hermes.consumer.engine.SubscribeHandle;
import com.ctrip.hermes.consumer.engine.bootstrap.strategy.ConsumingStrategy;
import com.ctrip.hermes.consumer.engine.bootstrap.strategy.ConsumingStrategyRegistry;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Partition;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = ConsumerBootstrap.class, value = Endpoint.BROKER)
public class BrokerConsumerBootstrap extends BaseConsumerBootstrap {

	@Inject
	private ConsumingStrategyRegistry m_consumingStrategyRegistry;

	@Override
	protected SubscribeHandle doStart(final ConsumerContext context) {

		CompositeSubscribeHandle handler = new CompositeSubscribeHandle();

		List<Partition> partitions = m_metaService.listPartitionsByTopic(context.getTopic().getName());
		ConsumingStrategy consumingStrategy = m_consumingStrategyRegistry.findStrategy(context.getConsumerType());
		for (final Partition partition : partitions) {
			handler.addSubscribeHandle(consumingStrategy.start(context, partition.getId()));
		}

		return handler;
	}
}
