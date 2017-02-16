package com.ctrip.hermes.core.pipeline;



public interface PipelineSink<T> {

	public T handle(PipelineContext<T> ctx, Object payload);

}
