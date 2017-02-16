package com.ctrip.hermes.producer.pipeline;

import java.util.concurrent.Future;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.pipeline.DefaultPipelineContext;
import com.ctrip.hermes.core.pipeline.Pipeline;
import com.ctrip.hermes.core.pipeline.PipelineContext;
import com.ctrip.hermes.core.pipeline.PipelineSink;
import com.ctrip.hermes.core.pipeline.ValveRegistry;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.producer.build.BuildConstants;

@Named(type = Pipeline.class, value = BuildConstants.PRODUCER)
public class ProducerPipeline implements Pipeline<Future<SendResult>> {
	@Inject(BuildConstants.PRODUCER)
	private ValveRegistry m_valveRegistry;

	@Inject
	private ProducerPipelineSinkManager m_sinkManager;

	@Override
	public Future<SendResult> put(Object payload) {
		ProducerMessage<?> msg = (ProducerMessage<?>) payload;

		String topic = msg.getTopic();
		PipelineSink<Future<SendResult>> sink = m_sinkManager.getSink(topic);
		PipelineContext<Future<SendResult>> ctx = new DefaultPipelineContext<Future<SendResult>>(m_valveRegistry.getValveList(), sink);

		ctx.next(msg);

		return ctx.getResult();
	}
}
