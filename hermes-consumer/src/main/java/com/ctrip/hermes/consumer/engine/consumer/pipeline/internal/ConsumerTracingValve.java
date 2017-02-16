package com.ctrip.hermes.consumer.engine.consumer.pipeline.internal;

import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.pipeline.PipelineContext;
import com.ctrip.hermes.core.pipeline.spi.Valve;

@Named(type = Valve.class, value = ConsumerTracingValve.ID)
public class ConsumerTracingValve implements Valve {

	public static final String ID = "consumer-tracing";

	@Override
	public void handle(PipelineContext<?> ctx, Object payload) {
		ctx.next(payload);
	}

}
