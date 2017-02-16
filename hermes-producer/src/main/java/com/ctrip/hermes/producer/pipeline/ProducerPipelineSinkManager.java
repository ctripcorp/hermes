package com.ctrip.hermes.producer.pipeline;

import java.util.concurrent.Future;

import com.ctrip.hermes.core.pipeline.PipelineSink;
import com.ctrip.hermes.core.result.SendResult;

public interface ProducerPipelineSinkManager {
	public PipelineSink<Future<SendResult>> getSink(String topic);
}
