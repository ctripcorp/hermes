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
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;
import com.ctrip.hermes.core.transport.command.v3.QueryLatestConsumerOffsetCommandV3;
import com.ctrip.hermes.core.transport.command.v3.QueryOffsetResultCommandV3;
import com.ctrip.hermes.env.config.broker.BrokerConfigProvider;

public class QueryLatestConsumerOffsetCommandProcessorV3 extends ContainerHolder implements CommandProcessor {

	private static final Logger log = LoggerFactory.getLogger(QueryLatestConsumerOffsetCommandProcessorV3.class);

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
		return Arrays.asList(CommandType.QUERY_LATEST_CONSUMER_OFFSET_V3);
	}

	@Override
	public void process(CommandProcessorContext ctx) {
		QueryLatestConsumerOffsetCommandV3 reqCmd = (QueryLatestConsumerOffsetCommandV3) ctx.getCommand();
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
					log.debug(
					      "No broker lease to handle client queryLatestOffset reqeust(correlationId={}, topic={}, partition={}, groupId={})",
					      correlationId, reqCmd.getTopic(), reqCmd.getPartition(), reqCmd.getGroupId());

				}
			} catch (Exception e) {
				log.error(
				      "Exception occurred while handling client queryLatestOffset reqeust(correlationId={}, topic={}, partition={}, groupId={})",
				      correlationId, reqCmd.getTopic(), reqCmd.getPartition(), reqCmd.getGroupId(), e);

				response(ctx.getChannel(), correlationId, null);
			}
		} else {
			log.debug("Consumer group not found for topic (correlationId={}, topic={}, partition={}, groupId={})",
			      correlationId, reqCmd.getTopic(), reqCmd.getPartition(), reqCmd.getGroupId());
		}
		response(ctx.getChannel(), correlationId, null);
	}

	private void response(Channel channel, long correlationId, Offset offset) {
		QueryOffsetResultCommandV3 cmd = new QueryOffsetResultCommandV3(offset);
		cmd.getHeader().setCorrelationId(correlationId);
		ChannelUtils.writeAndFlush(channel, cmd);
	}

}
