package com.ctrip.hermes.consumer.engine.bootstrap.strategy;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.consumer.ConsumerType;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = ConsumingStrategyRegistry.class)
public class DefaultConsumingRegistry extends ContainerHolder implements Initializable,
      ConsumingStrategyRegistry {

	private Map<ConsumerType, ConsumingStrategy> m_strategies = new ConcurrentHashMap<ConsumerType, ConsumingStrategy>();

	@Override
	public void initialize() throws InitializationException {
		Map<String, ConsumingStrategy> strategies = lookupMap(ConsumingStrategy.class);

		for (Map.Entry<String, ConsumingStrategy> entry : strategies.entrySet()) {
			m_strategies.put(ConsumerType.valueOf(entry.getKey()), entry.getValue());
		}
	}

	@Override
	public ConsumingStrategy findStrategy(ConsumerType consumerType) {
		return m_strategies.get(consumerType);
	}

}
