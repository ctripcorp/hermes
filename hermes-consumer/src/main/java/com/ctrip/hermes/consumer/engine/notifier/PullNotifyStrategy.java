package com.ctrip.hermes.consumer.engine.notifier;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.consumer.engine.ConsumerContext;
import com.ctrip.hermes.consumer.message.BrokerConsumerMessage;
import com.ctrip.hermes.consumer.pull.PullBrokerConsumerMessage;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.pipeline.Pipeline;

public class PullNotifyStrategy implements NotifyStrategy {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void notify(List<ConsumerMessage<?>> msgs, ConsumerContext context, ExecutorService executorService,
	      Pipeline<Void> pipeline) {

		List<ConsumerMessage<?>> decoratedMsgs = new ArrayList<>(msgs.size());
		for (ConsumerMessage<?> msg : msgs) {
			decoratedMsgs.add(new PullBrokerConsumerMessage<>((BrokerConsumerMessage) msg));
		}

		pipeline.put(new Pair<ConsumerContext, List<ConsumerMessage<?>>>(context, decoratedMsgs));
	}

}
