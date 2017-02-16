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
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.transport.ChannelUtils;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.QueryMessageOffsetByTimeCommand;
import com.ctrip.hermes.core.transport.command.QueryOffsetResultCommand;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;
import com.ctrip.hermes.env.config.broker.BrokerConfigProvider;

public class QueryMessageOffsetByTimeCommandProcessor extends ContainerHolder implements CommandProcessor {
	private static final Logger log = LoggerFactory.getLogger(QueryMessageOffsetByTimeCommandProcessor.class);

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
		return Arrays.asList(CommandType.QUERY_MESSAGE_OFFSET_BY_TIME);
	}

	// return null[when error] or (0, 0)[when not found]
	@Override
	public void process(CommandProcessorContext ctx) {
		QueryMessageOffsetByTimeCommand reqCmd = (QueryMessageOffsetByTimeCommand) ctx.getCommand();
		long correlationId = reqCmd.getHeader().getCorrelationId();
		String topic = reqCmd.getTopic();
		int partition = reqCmd.getPartition();
		long time = reqCmd.getTime();

		try {
			if (m_leaseContainer.acquireLease(topic, partition, m_config.getSessionId()) != null) {
				Offset offset = m_messageQueueManager.findMessageOffsetByTime(topic, partition, time);
				response(ctx.getChannel(), correlationId, offset);
				return;
			} else {
				logDebug(reqCmd, "No broker lease to handle client request.");
			}
		} catch (Exception e) {
			logDebugWithError(reqCmd, "Find message offset failed.", e);
		}
		response(ctx.getChannel(), correlationId, null);
	}

	private void response(Channel channel, long correlationId, Offset offset) {
		QueryOffsetResultCommand cmd = new QueryOffsetResultCommand(offset);
		cmd.getHeader().setCorrelationId(correlationId);
		ChannelUtils.writeAndFlush(channel, cmd);
	}

	private void logDebug(QueryMessageOffsetByTimeCommand cmd, String debugInfo) {
		if (log.isDebugEnabled()) {
			log.debug(debugInfo + " (correlationId={}, topic={}, partition={}, time={})", cmd.getHeader()
			      .getCorrelationId(), cmd.getTopic(), cmd.getPartition(), cmd.getTime());
		}
	}

	private void logDebugWithError(QueryMessageOffsetByTimeCommand cmd, String errorInfo, Exception e) {
		log.debug(errorInfo + " (correlationId={}, topic={}, partition={}, time={})", cmd.getHeader().getCorrelationId(),
		      cmd.getTopic(), cmd.getPartition(), cmd.getTime(), e);
	}

}
