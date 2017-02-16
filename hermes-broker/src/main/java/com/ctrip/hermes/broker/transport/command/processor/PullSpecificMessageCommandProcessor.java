package com.ctrip.hermes.broker.transport.command.processor;

import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.broker.lease.BrokerLeaseContainer;
import com.ctrip.hermes.broker.queue.MessageQueueManager;
import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch;
import com.ctrip.hermes.core.transport.ChannelUtils;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;
import com.ctrip.hermes.core.transport.command.v2.PullSpecificMessageCommand;
import com.ctrip.hermes.core.transport.command.v3.PullMessageResultCommandV3;
import com.ctrip.hermes.env.config.broker.BrokerConfigProvider;

public class PullSpecificMessageCommandProcessor implements CommandProcessor {
	private static final Logger log = LoggerFactory.getLogger(PullSpecificMessageCommandProcessor.class);

	@Inject
	private MessageQueueManager m_messageQueueManager;

	@Inject
	private BrokerLeaseContainer m_leaseContainer;

	@Inject
	private BrokerConfigProvider m_config;

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.MESSAGE_PULL_SPECIFIC);
	}

	@Override
	public void process(CommandProcessorContext ctx) {
		PullSpecificMessageCommand reqCmd = (PullSpecificMessageCommand) ctx.getCommand();
		long correlationId = reqCmd.getHeader().getCorrelationId();
		String topic = reqCmd.getTopic();
		int partition = reqCmd.getPartition();
		List<Offset> offsets = reqCmd.getOffsets();

		try {
			Lease lease = m_leaseContainer.acquireLease(topic, partition, m_config.getSessionId());
			if (lease != null) {
				List<TppConsumerMessageBatch> batches = m_messageQueueManager.findMessagesByOffsets( //
				      topic, partition, offsets);
				if (!batches.isEmpty()) {
					PullMessageResultCommandV3 cmd = new PullMessageResultCommandV3();
					cmd.setBrokerAccepted(true);
					cmd.addBatches(batches);
					cmd.getHeader().setCorrelationId(correlationId);
					ChannelUtils.writeAndFlush(ctx.getChannel(), cmd);
					return;
				}
			} else {
				log.debug("No broker lease to handle pull message cmd. [op-id: {}] {} {}", correlationId, topic, partition);
			}
		} catch (Exception e) {
			log.debug("Handle pull message reqeust failed. [op-id: {}] {} {}", correlationId, topic, partition, e);
		}
		PullMessageResultCommandV3 cmd = new PullMessageResultCommandV3();
		cmd.setBrokerAccepted(false);
		cmd.getHeader().setCorrelationId(correlationId);
		ChannelUtils.writeAndFlush(ctx.getChannel(), cmd);
	}
}
