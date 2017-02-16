package com.ctrip.hermes.broker.transport.command.processor;

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
import com.ctrip.hermes.core.transport.command.Command;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.PullMessageCommand;
import com.ctrip.hermes.core.transport.command.PullMessageResultCommand;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;
import com.ctrip.hermes.core.transport.command.v2.PullMessageCommandV2;
import com.ctrip.hermes.core.transport.command.v2.PullMessageResultCommandV2;
import com.ctrip.hermes.core.utils.CatUtil;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.env.config.broker.BrokerConfigProvider;

public class PullMessageCommandProcessor implements CommandProcessor {

	private static final Logger log = LoggerFactory.getLogger(PullMessageCommandProcessor.class);

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
		return Arrays.asList(CommandType.MESSAGE_PULL, CommandType.MESSAGE_PULL_V2);
	}

	@Override
	public void process(CommandProcessorContext ctx) {
		int version = ctx.getCommand().getHeader().getVersion();
		PullMessageTask task = 1 == version ? preparePullMessageTask(ctx) : preparePullMessageTaskV2(ctx);
		Tpg tpg = task.getTpg();

		try {
			String topic = tpg.getTopic();
			int partition = tpg.getPartition();
			if (m_metaService.containsConsumerGroup(topic, tpg.getGroupId())) {
				logReqToCat(ctx, tpg);

				Lease lease = m_leaseContainer.acquireLease(topic, partition, m_config.getSessionId());
				if (lease != null) {
					task.setBrokerLease(lease);
					m_longPollingService.schedulePush(task);
					return;
				} else {
					logDebug("No broker lease to handle client pull message reqeust.", task.getCorrelationId(), tpg, null);
				}
			} else {
				logDebug("Consumer group not found for topic.", task.getCorrelationId(), tpg, null);
			}
		} catch (Exception e) {
			logDebug("Exception occurred while handling client pull message reqeust.", task.getCorrelationId(), tpg, e);
		}
		// can not acquire lease, response with empty result
		responseError(task);
	}

	private void logReqToCat(CommandProcessorContext ctx, Tpg tpg) {
		CatUtil.logEventPeriodically(CatConstants.TYPE_PULL_CMD + ctx.getCommand().getHeader().getType().getVersion(),
		      tpg.getTopic() + "-" + tpg.getPartition() + "-" + tpg.getGroupId());
	}

	private void logDebug(String debugInfo, long correlationId, Tpg tpg, Exception e) {
		if (log.isDebugEnabled()) {
			if (e != null) {
				log.debug(debugInfo + " [correlation id: {}] {}", correlationId, tpg, e);
			} else {
				log.debug(debugInfo + " [correlation id: {}] {}", correlationId, tpg);
			}
		}
	}

	private void responseError(PullMessageTask task) {
		Command cmd = null;
		switch (task.getPullMessageCommandVersion()) {
		case 1:
			cmd = new PullMessageResultCommand();
			break;
		case 2:
			cmd = new PullMessageResultCommandV2();
			((PullMessageResultCommandV2) cmd).setBrokerAccepted(false);
			break;
		}
		cmd.getHeader().setCorrelationId(task.getCorrelationId());
		ChannelUtils.writeAndFlush(task.getChannel(), cmd);
	}

	private PullMessageTask preparePullMessageTask(CommandProcessorContext ctx) {
		PullMessageCommand cmd = (PullMessageCommand) ctx.getCommand();
		PullMessageTask task = new PullMessageTask(new Date(cmd.getReceiveTime()), null);

		task.setPullCommandVersion(1);

		task.setBatchSize(cmd.getSize());
		task.setChannel(ctx.getChannel());
		task.setCorrelationId(cmd.getHeader().getCorrelationId());
		task.setExpireTime(cmd.getExpireTime());
		task.setTpg(new Tpg(cmd.getTopic(), cmd.getPartition(), cmd.getGroupId()));
		task.setClientIp(ctx.getRemoteIp());

		return task;
	}

	private PullMessageTask preparePullMessageTaskV2(CommandProcessorContext ctx) {
		PullMessageCommandV2 cmd = (PullMessageCommandV2) ctx.getCommand();
		PullMessageTask task = new PullMessageTask(new Date(cmd.getReceiveTime()), null);

		task.setPullCommandVersion(2);

		task.setBatchSize(cmd.getSize());
		task.setChannel(ctx.getChannel());
		task.setCorrelationId(cmd.getHeader().getCorrelationId());
		String timeRel = cmd.getHeader().getProperties().get("timeout-rel");
		if (StringUtils.isBlank(timeRel) || !"true".equals(timeRel)) {
			task.setExpireTime(cmd.getExpireTime());
		} else {
			task.setExpireTime(cmd.getExpireTime() + System.currentTimeMillis() - 20L);
		}
		task.setTpg(new Tpg(cmd.getTopic(), cmd.getPartition(), cmd.getGroupId()));
		task.setClientIp(ctx.getRemoteIp());

		task.setStartOffset(cmd.getOffset());
		task.setWithOffset(PullMessageCommandV2.PULL_WITH_OFFSET == cmd.getPullType());

		return task;
	}
}
