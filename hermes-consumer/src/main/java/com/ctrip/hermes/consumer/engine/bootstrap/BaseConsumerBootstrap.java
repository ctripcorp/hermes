package com.ctrip.hermes.consumer.engine.bootstrap;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.consumer.engine.ConsumerContext;
import com.ctrip.hermes.consumer.engine.SubscribeHandle;
import com.ctrip.hermes.consumer.engine.notifier.ConsumerNotifier;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.transport.endpoint.EndpointClient;
import com.ctrip.hermes.core.transport.endpoint.EndpointManager;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public abstract class BaseConsumerBootstrap implements ConsumerBootstrap {

	@Inject
	protected EndpointClient m_endpointClient;

	@Inject
	protected EndpointManager m_endpointManager;

	@Inject
	protected MetaService m_metaService;

	@Inject
	protected ConsumerNotifier m_consumerNotifier;

	public SubscribeHandle start(ConsumerContext context) {
		return doStart(context);
	}

	public void stop(ConsumerContext context) {
		doStop(context);
	}

	protected void doStop(ConsumerContext context) {

	}

	protected abstract SubscribeHandle doStart(ConsumerContext context);

}
