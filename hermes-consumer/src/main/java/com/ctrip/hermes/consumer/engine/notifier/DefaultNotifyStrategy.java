package com.ctrip.hermes.consumer.engine.notifier;

import java.util.List;
import java.util.concurrent.ExecutorService;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.consumer.engine.ConsumerContext;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.pipeline.Pipeline;

public class DefaultNotifyStrategy implements NotifyStrategy {

	@Override
	public void notify(List<ConsumerMessage<?>> msgs, ConsumerContext context, ExecutorService executorService,
	      Pipeline<Void> pipeline) {
		pipeline.put(new Pair<ConsumerContext, List<ConsumerMessage<?>>>(context, msgs));
	}

}
