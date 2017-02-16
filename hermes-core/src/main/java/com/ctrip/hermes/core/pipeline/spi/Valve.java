package com.ctrip.hermes.core.pipeline.spi;

import com.ctrip.hermes.core.pipeline.PipelineContext;

public interface Valve {

	public void handle(PipelineContext<?> ctx, Object payload);

}
