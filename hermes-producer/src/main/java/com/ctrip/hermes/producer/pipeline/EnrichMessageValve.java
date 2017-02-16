package com.ctrip.hermes.producer.pipeline;

import java.util.UUID;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.net.Networks;

import com.ctrip.hermes.core.message.MessagePropertyNames;
import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.pipeline.PipelineContext;
import com.ctrip.hermes.core.pipeline.spi.Valve;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.producer.config.ProducerConfig;

@SuppressWarnings("deprecation")
@Named(type = Valve.class, value = EnrichMessageValve.ID)
public class EnrichMessageValve implements Valve, Initializable {
	private static final Logger log = LoggerFactory.getLogger(EnrichMessageValve.class);

	public static final String ID = "enrich";

	@Inject
	private ProducerConfig m_config;

	private boolean m_logEnrichInfo = false;

	@Override
	public void handle(PipelineContext<?> ctx, Object payload) {
		ProducerMessage<?> msg = (ProducerMessage<?>) payload;
		String ip = Networks.forIp().getLocalHostAddress();
		enrichPartitionKey(msg);

		enrichRefKey(msg);
		enrichMessageProperties(msg, ip);
		ctx.next(payload);
	}

	private void enrichRefKey(ProducerMessage<?> msg) {
		if (StringUtils.isEmpty(msg.getKey())) {
			String refKey = UUID.randomUUID().toString();
			if (m_logEnrichInfo) {
				log.info("Ref key not set, will set uuid as ref key(topic={}, ref key={})", msg.getTopic(), refKey);
			}
			msg.setKey(refKey);
		}
	}

	private void enrichMessageProperties(ProducerMessage<?> msg, String ip) {
		msg.addDurableSysProperty(MessagePropertyNames.PRODUCER_IP, ip);
	}

	private void enrichPartitionKey(ProducerMessage<?> msg) {
		if (StringUtils.isEmpty(msg.getPartitionKey())) {
			if (m_logEnrichInfo) {
				log.info("Parition key not set, will set random key as partition key(topic={})", msg.getTopic());
			}
			msg.setPartitionKey(UUID.randomUUID().toString());
		}
	}

	@Override
	public void initialize() throws InitializationException {
		m_logEnrichInfo = m_config.isLogEnrichInfoEnabled();
	}

}
