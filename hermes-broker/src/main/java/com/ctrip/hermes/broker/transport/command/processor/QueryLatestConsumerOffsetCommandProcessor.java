package com.ctrip.hermes.broker.transport.command.processor;

import io.netty.channel.Channel;

import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.broker.lease.BrokerLeaseContainer;
import com.ctrip.hermes.broker.queue.MessageQueueManager;
import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.transport.ChannelUtils;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.QueryLatestConsumerOffsetCommand;
import com.ctrip.hermes.core.transport.command.QueryOffsetResultCommand;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;
import com.ctrip.hermes.env.config.broker.BrokerConfigProvider;

public class QueryLatestConsumerOffsetCommandProcessor extends ContainerHolder implements CommandProcessor {

	private static final Logger log = LoggerFactory.getLogger(QueryLatestConsumerOffsetCommandProcessor.class);

	@Inject
	private MetaService m_metaService;

	@Inject
	private BrokerLeaseContainer m_leaseContainer;

	@Inject
	private BrokerConfigProvider m_config;

	@Inject
	private MessageQueueManager m_messageQueueManager;

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.QUERY_LATEST_CONSUMER_OFFSET);
	}

	@Override
	public void process(CommandProcessorContext ctx) {
		QueryLatestConsumerOffsetCommand reqCmd = (QueryLatestConsumerOffsetCommand) ctx.getCommand();
		long correlationId = reqCmd.getHeader().getCorrelationId();
		String topic = reqCmd.getTopic();
		String groupId = reqCmd.getGroupId();
		int partition = reqCmd.getPartition();

		if (m_metaService.containsConsumerGroup(topic, groupId)) {
			try {
				if (m_leaseContainer.acquireLease(topic, partition, m_config.getSessionId()) != null) {
					Offset offset = m_messageQueueManager.findLatestConsumerOffset(new Tpg(topic, partition, groupId));
					response(ctx.getChannel(), correlationId, offset);
					return;
				} else {
					logDebug(reqCmd, "No broker lease to handle client request.");
				}
			} catch (Exception e) {
				logError(reqCmd, "Find offset failed.", e);
			}
		} else {
			logDebug(reqCmd, "No consumer group was found.");
		}
		response(ctx.getChannel(), correlationId, null);
	}

	private void response(Channel channel, long correlationId, Offset offset) {
		QueryOffsetResultCommand cmd = new QueryOffsetResultCommand(offset);
		cmd.getHeader().setCorrelationId(correlationId);
		ChannelUtils.writeAndFlush(channel, cmd);
	}

	private void logDebug(QueryLatestConsumerOffsetCommand cmd, String debugInfo) {
		if (log.isDebugEnabled()) {
			log.debug(debugInfo + " (correlationId={}, topic={}, partition={}, groupId={})", cmd.getHeader()
			      .getCorrelationId(), cmd.getTopic(), cmd.getPartition(), cmd.getGroupId());
		}
	}

	private void logError(QueryLatestConsumerOffsetCommand cmd, String errorInfo, Exception e) {
		log.error(errorInfo + " (correlationId={}, topic={}, partition={}, groupId={})", cmd.getHeader()
		      .getCorrelationId(), cmd.getTopic(), cmd.getPartition(), cmd.getGroupId(), e);
	}
}
