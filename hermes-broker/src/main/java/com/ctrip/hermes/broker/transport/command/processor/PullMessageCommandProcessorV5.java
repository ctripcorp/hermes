package com.ctrip.hermes.broker.transport.command.processor;

import io.netty.channel.Channel;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.broker.lease.BrokerLeaseContainer;
import com.ctrip.hermes.broker.longpolling.LongPollingService;
import com.ctrip.hermes.broker.longpolling.PullMessageTask;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.constants.CatConstants;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.transport.ChannelUtils;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;
import com.ctrip.hermes.core.transport.command.v5.PullMessageAckCommandV5;
import com.ctrip.hermes.core.transport.command.v5.PullMessageCommandV5;
import com.ctrip.hermes.core.utils.CatUtil;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.env.config.broker.BrokerConfigProvider;
import com.ctrip.hermes.meta.entity.Endpoint;

public class PullMessageCommandProcessorV5 implements CommandProcessor {

	private static final Logger log = LoggerFactory.getLogger(PullMessageCommandProcessorV5.class);

	@Inject
	private LongPollingService m_longPollingService;

	@Inject
	private BrokerLeaseContainer m_leaseContainer;

	@Inject
	private BrokerConfigProvider m_config;

	@Inject
	private MetaService m_metaService;

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.MESSAGE_PULL_V5);
	}

	@Override
	public void process(CommandProcessorContext ctx) {

		PullMessageCommandV5 reqCmd = (PullMessageCommandV5) ctx.getCommand();

		long correlationId = reqCmd.getHeader().getCorrelationId();

		String topic = reqCmd.getTopic();
		int partition = reqCmd.getPartition();
		String groupId = reqCmd.getGroupId();
		try {
			if (m_metaService.containsConsumerGroup(topic, groupId)) {
				logReqToCat(reqCmd);

				Lease lease = m_leaseContainer.acquireLease(topic, partition, m_config.getSessionId());
				if (lease != null) {
					responseAck(ctx.getChannel(), reqCmd, true);
					PullMessageTask task = createPullMessageTask(reqCmd, lease, ctx.getChannel(), ctx.getRemoteIp());
					m_longPollingService.schedulePush(task);
					return;
				} else {
					log.debug(
					      "No broker lease to handle client pull message reqeust(correlationId={}, topic={}, partition={}, groupId={})",
					      correlationId, topic, partition, groupId);
				}
			} else {
				log.debug("Consumer group not found for topic (correlationId={}, topic={}, partition={}, groupId={})",
				      correlationId, topic, partition, groupId);
			}
		} catch (Exception e) {
			log.debug(
			      "Exception occurred while handling client pull message reqeust(correlationId={}, topic={}, partition={}, groupId={})",
			      correlationId, topic, partition, groupId, e);
		}

		responseAck(ctx.getChannel(), reqCmd, false);
	}

	private void responseAck(Channel channel, PullMessageCommandV5 reqCmd, boolean success) {
		PullMessageAckCommandV5 ack = new PullMessageAckCommandV5();
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

	private void logReqToCat(PullMessageCommandV5 reqCmd) {
		CatUtil.logEventPeriodically(CatConstants.TYPE_PULL_CMD + reqCmd.getHeader().getType().getVersion(),
		      reqCmd.getTopic() + "-" + reqCmd.getPartition() + "-" + reqCmd.getGroupId());
	}

	private PullMessageTask createPullMessageTask(PullMessageCommandV5 cmd, Lease brokerLease, Channel channel,
	      String clientIp) {
		String pullTimeStr = cmd.getHeader().getProperties().get("pullTime");

		PullMessageTask task = new PullMessageTask(new Date(cmd.getReceiveTime()),
		      StringUtils.isNumeric(pullTimeStr) ? new Date(Long.parseLong(pullTimeStr)) : null);

		task.setBatchSize(cmd.getSize());
		task.setBrokerLease(brokerLease);
		task.setChannel(channel);
		task.setCorrelationId(cmd.getHeader().getCorrelationId());
		task.setExpireTime(cmd.getExpireTime() + System.currentTimeMillis() - 20L);
		task.setPullCommandVersion(5);
		task.setWithOffset(true);
		task.setStartOffset(cmd.getOffset());
		task.setTpg(new Tpg(cmd.getTopic(), cmd.getPartition(), cmd.getGroupId()));
		task.setClientIp(clientIp);
		task.setFilter(cmd.getFilter());

		return task;
	}
}
