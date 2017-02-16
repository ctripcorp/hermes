package com.ctrip.hermes.broker.transport.command.processor;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.unidal.lookup.annotation.Inject;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.broker.biz.logger.BrokerFileBizLogger;
import com.ctrip.hermes.broker.lease.BrokerLeaseContainer;
import com.ctrip.hermes.broker.queue.AckMessagesTask;
import com.ctrip.hermes.broker.queue.MessageQueueManager;
import com.ctrip.hermes.core.bo.AckContext;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.log.BizEvent;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.core.transport.ChannelUtils;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;
import com.ctrip.hermes.core.transport.command.processor.ThreadCount;
import com.ctrip.hermes.core.transport.command.v5.AckMessageAckCommandV5;
import com.ctrip.hermes.core.transport.command.v5.AckMessageCommandV5;
import com.ctrip.hermes.core.utils.CollectionUtil;
import com.ctrip.hermes.env.config.broker.BrokerConfigProvider;
import com.ctrip.hermes.meta.entity.Endpoint;

import io.netty.channel.Channel;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@ThreadCount(value = 20)
public class AckMessageCommandProcessorV5 implements CommandProcessor {

	@Inject
	private BrokerFileBizLogger m_bizLogger;

	@Inject
	private MessageQueueManager m_messageQueueManager;

	@Inject
	private MetaService m_metaService;

	@Inject
	private SystemClockService m_systemClockService;

	@Inject
	private BrokerLeaseContainer m_leaseContainer;

	@Inject
	private BrokerConfigProvider m_config;

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.MESSAGE_ACK_V5);
	}

	@Override
	public void process(CommandProcessorContext ctx) {
		AckMessageCommandV5 reqCmd = (AckMessageCommandV5) ctx.getCommand();

		String topic = reqCmd.getTopic();
		int partition = reqCmd.getPartition();
		String groupId = reqCmd.getGroup();

		Lease lease = m_leaseContainer.acquireLease(topic, partition, m_config.getSessionId());

		if (lease != null) {
			responseAck(ctx.getChannel(), reqCmd, true);

			List<AckContext> ackedPriorityContexts = reqCmd.getAckedMsgs().get(0);
			List<AckContext> ackedContexts = reqCmd.getAckedMsgs().get(1);
			List<AckContext> ackedResendContexts = reqCmd.getAckedResendMsgs().get(1);

			List<AckContext> nackedPriorityContexts = reqCmd.getNackedMsgs().get(0);
			List<AckContext> nackedContexts = reqCmd.getNackedMsgs().get(1);
			List<AckContext> nackedResendContexts = reqCmd.getNackedResendMsgs().get(1);

			logAcked(ctx.getRemoteIp(), topic, partition, groupId, ackedPriorityContexts, ackedContexts,
			      ackedResendContexts);
			logNacked(ctx.getRemoteIp(), topic, partition, groupId, nackedPriorityContexts, nackedContexts,
			      nackedResendContexts);

			AckMessagesTask task = new AckMessagesTask(topic, partition, 5, groupId,
			      reqCmd.getHeader().getCorrelationId(), ctx.getChannel(), m_systemClockService.now()
			            + reqCmd.getTimeout());
			task.setAckedContexts(ackedContexts);
			task.setAckedPriorityContexts(ackedPriorityContexts);
			task.setAckedResendContexts(ackedResendContexts);
			task.setNackedContexts(nackedContexts);
			task.setNackedPriorityContexts(nackedPriorityContexts);
			task.setNackedResendContexts(nackedResendContexts);
			m_messageQueueManager.submitAckMessagesTask(task);
		} else {
			responseAck(ctx.getChannel(), reqCmd, false);
		}
	}

	private void responseAck(Channel channel, AckMessageCommandV5 reqCmd, boolean success) {
		AckMessageAckCommandV5 ack = new AckMessageAckCommandV5();
		ack.correlate(reqCmd);
		ack.setSuccess(success);
		if (!success && m_metaService.findTopicByName(reqCmd.getTopic()) != null) {
			Pair<Endpoint, Long> endpointEntry = m_metaService.findEndpointByTopicAndPartition(reqCmd.getTopic(),
			      reqCmd.getPartition());
			if (endpointEntry != null) {
				ack.setNewEndpoint(endpointEntry.getKey());
			}
		}

		ChannelUtils.writeAndFlush(channel, ack);
	}

	private void logNacked(String consumerIp, String topic, int partition, String groupId,
	      List<AckContext> nackedPriorityContexts, List<AckContext> nackedContexts, List<AckContext> nackedResendContexts) {
		// priority
		bizLog(new Tpp(topic, partition, true), consumerIp, groupId, nackedPriorityContexts, false, false);
		// non-priority
		bizLog(new Tpp(topic, partition, false), consumerIp, groupId, nackedContexts, false, false);
		// resend
		bizLog(new Tpp(topic, partition, false), consumerIp, groupId, nackedResendContexts, true, false);
	}

	private void logAcked(String consumerIp, String topic, int partition, String groupId,
	      List<AckContext> ackedPriorityContexts, List<AckContext> ackedContexts, List<AckContext> ackedResendContexts) {
		// priority
		bizLog(new Tpp(topic, partition, true), consumerIp, groupId, ackedPriorityContexts, false, true);
		// non-priority
		bizLog(new Tpp(topic, partition, false), consumerIp, groupId, ackedContexts, false, true);
		// resend
		bizLog(new Tpp(topic, partition, false), consumerIp, groupId, ackedResendContexts, true, true);
	}

	private void bizLog(Tpp tpp, String consumerIp, String groupId, List<AckContext> ackContexts, boolean isResend,
	      boolean ack) {
		if (CollectionUtil.isNotEmpty(ackContexts)) {
			for (AckContext ctx : ackContexts) {
				BizEvent ackEvent = new BizEvent("Message.Acked");
				addBizData(ackEvent, tpp, consumerIp, groupId, ctx, isResend, ack);
				addConsumerProcessTime(ackEvent, ctx.getOnMessageEndTimeMillis() - ctx.getOnMessageStartTimeMillis());
				ackEvent.addData("BizStart", new Date(ctx.getOnMessageStartTimeMillis()));
				ackEvent.addData("BizEnd", new Date(ctx.getOnMessageEndTimeMillis()));
				m_bizLogger.log(ackEvent);
			}
		}
	}

	private void addConsumerProcessTime(BizEvent event, long processTime) {
		event.addData("processTime", processTime);
	}

	private void addBizData(BizEvent event, Tpp tpp, String consumerIp, String groupId, AckContext ctx,
	      boolean isResend, boolean ack) {
		event.addData("topic", m_metaService.findTopicByName(tpp.getTopic()).getId());
		event.addData("partition", tpp.getPartition());
		event.addData("priority", tpp.getPriorityInt());
		event.addData("msgId", ctx.getMsgSeq());
		event.addData("consumerIp", consumerIp);
		event.addData("groupId", m_metaService.translateToIntGroupId(tpp.getTopic(), groupId));
		event.addData("isResend", isResend);
		event.addData("ack", ack);
		if (isResend) {
			event.addData("remainingRetries", ctx.getRemainingRetries());
		}
	}
}
