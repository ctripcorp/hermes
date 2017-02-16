package com.ctrip.hermes.producer.pipeline;

import java.util.concurrent.Future;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.core.constants.CatConstants;
import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.pipeline.PipelineContext;
import com.ctrip.hermes.core.pipeline.PipelineSink;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.env.HermesVersion;
import com.ctrip.hermes.producer.config.ProducerConfig;
import com.ctrip.hermes.producer.sender.MessageSender;
import com.dianping.cat.status.ProductVersionManager;

public class DefaultProducerPipelineSink implements PipelineSink<Future<SendResult>>, Initializable {
	@Inject
	private MessageSender messageSender;

	@Inject
	private ProducerConfig m_config;

	@Override
	public Future<SendResult> handle(PipelineContext<Future<SendResult>> ctx, Object input) {
		return messageSender.send((ProducerMessage<?>) input);
	}

	@Override
	public void initialize() throws InitializationException {
		ProductVersionManager.getInstance().register(CatConstants.TYPE_HERMES_CLIENT_VERSION, HermesVersion.CURRENT);
	}

}
