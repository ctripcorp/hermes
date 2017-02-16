package com.ctrip.hermes.broker.transport.command.processor;

import io.netty.channel.Channel;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;

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
import com.ctrip.hermes.core.transport.command.v4.PullMessageCommandV4;
import com.ctrip.hermes.core.transport.command.v4.PullMessageResultCommandV4;
import com.ctrip.hermes.core.utils.CatUtil;
import com.ctrip.hermes.env.config.broker.BrokerConfigProvider;

public class PullMessageCommandProcessorV4 implements CommandProcessor {

	private static final Logger log = LoggerFactory.getLogger(PullMessageCommandProcessorV4.class);

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
		return Arrays.asList(CommandType.MESSAGE_PULL_V4);
	}

	@Override
	public void process(CommandProcessorContext ctx) {

		PullMessageCommandV4 reqCmd = (PullMessageCommandV4) ctx.getCommand();

		long correlationId = reqCmd.getHeader().getCorrelationId();

		String topic = reqCmd.getTopic();
		int partition = reqCmd.getPartition();
		try {
			if (m_metaService.containsConsumerGroup(topic, reqCmd.getGroupId())) {
				logReqToCat(reqCmd);

				Lease lease = m_leaseContainer.acquireLease(topic, partition, m_config.getSessionId());
				if (lease != null) {

					PullMessageTask task = createPullMessageTask(reqCmd, lease, ctx.getChannel(), ctx.getRemoteIp());
					m_longPollingService.schedulePush(task);
					return;
				} else {
					log.debug(
					      "No broker lease to handle client pull message reqeust(correlationId={}, topic={}, partition={}, groupId={})",
					      correlationId, topic, partition, reqCmd.getGroupId());
				}
			} else {
				log.debug("Consumer group not found for topic (correlationId={}, topic={}, partition={}, groupId={})",
				      correlationId, topic, partition, reqCmd.getGroupId());
			}
		} catch (Exception e) {
			log.debug(
			      "Exception occurred while handling client pull message reqeust(correlationId={}, topic={}, partition={}, groupId={})",
			      correlationId, topic, partition, reqCmd.getGroupId(), e);
		}

		// can not acquire lease, response with empty result
		PullMessageResultCommandV4 cmd = new PullMessageResultCommandV4();
		cmd.getHeader().setCorrelationId(reqCmd.getHeader().getCorrelationId());
		cmd.setBrokerAccepted(false);

		ChannelUtils.writeAndFlush(ctx.getChannel(), cmd);

	}

	private void logReqToCat(PullMessageCommandV4 reqCmd) {
		CatUtil.logEventPeriodically(CatConstants.TYPE_PULL_CMD + reqCmd.getHeader().getType().getVersion(),
		      reqCmd.getTopic() + "-" + reqCmd.getPartition() + "-" + reqCmd.getGroupId());
	}

	private PullMessageTask createPullMessageTask(PullMessageCommandV4 cmd, Lease brokerLease, Channel channel,
	      String clientIp) {
		PullMessageTask task = new PullMessageTask(new Date(cmd.getReceiveTime()), null);

		task.setBatchSize(cmd.getSize());
		task.setBrokerLease(brokerLease);
		task.setChannel(channel);
		task.setCorrelationId(cmd.getHeader().getCorrelationId());
		task.setExpireTime(cmd.getExpireTime() + System.currentTimeMillis() - 20L);
		task.setPullCommandVersion(4);
		task.setWithOffset(true);
		task.setStartOffset(cmd.getOffset());
		task.setTpg(new Tpg(cmd.getTopic(), cmd.getPartition(), cmd.getGroupId()));
		task.setClientIp(clientIp);
		task.setFilter(cmd.getFilter());

		return task;
	}
}
