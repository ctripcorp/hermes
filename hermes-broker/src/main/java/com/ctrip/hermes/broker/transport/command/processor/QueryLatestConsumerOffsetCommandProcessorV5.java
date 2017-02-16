package com.ctrip.hermes.broker.transport.command.processor;

import io.netty.channel.Channel;

import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Inject;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.broker.lease.BrokerLeaseContainer;
import com.ctrip.hermes.broker.queue.MessageQueueManager;
import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.transport.ChannelUtils;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;
import com.ctrip.hermes.core.transport.command.v5.QueryLatestConsumerOffsetAckCommandV5;
import com.ctrip.hermes.core.transport.command.v5.QueryLatestConsumerOffsetCommandV5;
import com.ctrip.hermes.core.transport.command.v5.QueryOffsetResultCommandV5;
import com.ctrip.hermes.env.config.broker.BrokerConfigProvider;
import com.ctrip.hermes.meta.entity.Endpoint;

public class QueryLatestConsumerOffsetCommandProcessorV5 extends ContainerHolder implements CommandProcessor {

	private static final Logger log = LoggerFactory.getLogger(QueryLatestConsumerOffsetCommandProcessorV5.class);

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
		return Arrays.asList(CommandType.QUERY_LATEST_CONSUMER_OFFSET_V5);
	}

	@Override
	public void process(CommandProcessorContext ctx) {
		QueryLatestConsumerOffsetCommandV5 reqCmd = (QueryLatestConsumerOffsetCommandV5) ctx.getCommand();
		long correlationId = reqCmd.getHeader().getCorrelationId();

		String topic = reqCmd.getTopic();
		int partition = reqCmd.getPartition();
		String groupId = reqCmd.getGroupId();

		if (m_metaService.containsConsumerGroup(topic, groupId)) {
			try {
				if (m_leaseContainer.acquireLease(topic, partition, m_config.getSessionId()) != null) {
					responseAck(ctx.getChannel(), reqCmd, true);
					Offset offset = m_messageQueueManager.findLatestConsumerOffset(new Tpg(topic, partition, groupId));
					responseResult(ctx.getChannel(), reqCmd, offset);
					return;
				} else {
					log.debug(
					      "No broker lease to handle client queryLatestOffset reqeust(correlationId={}, topic={}, partition={}, groupId={})",
					      correlationId, topic, partition, groupId);

				}
			} catch (Exception e) {
				log.error(
				      "Exception occurred while handling client queryLatestOffset reqeust(correlationId={}, topic={}, partition={}, groupId={})",
				      correlationId, topic, partition, groupId, e);
			}
		} else {
			log.debug("Consumer group not found for topic (correlationId={}, topic={}, partition={}, groupId={})",
			      correlationId, topic, partition, groupId);
		}
		responseAck(ctx.getChannel(), reqCmd, false);
	}

	private void responseAck(Channel channel, QueryLatestConsumerOffsetCommandV5 reqCmd, boolean success) {
		QueryLatestConsumerOffsetAckCommandV5 ack = new QueryLatestConsumerOffsetAckCommandV5();
		ack.correlate(reqCmd);
		ack.setSuccess(success);

		String topic = reqCmd.getTopic();
		int partition = reqCmd.getPartition();
		String groupId = reqCmd.getGroupId();

		if (!success && m_metaService.containsConsumerGroup(topic, groupId)) {
			Pair<Endpoint, Long> endpointEntry = m_metaService.findEndpointByTopicAndPartition(topic, partition);
			if (endpointEntry != null) {
				ack.setNewEndpoint(endpointEntry.getKey());
			}
		}

		ChannelUtils.writeAndFlush(channel, ack);
	}

	private void responseResult(Channel channel, QueryLatestConsumerOffsetCommandV5 reqCmd, Offset offset) {
		QueryOffsetResultCommandV5 cmd = new QueryOffsetResultCommandV5(offset);
		cmd.correlate(reqCmd);
		ChannelUtils.writeAndFlush(channel, cmd);
	}

}
