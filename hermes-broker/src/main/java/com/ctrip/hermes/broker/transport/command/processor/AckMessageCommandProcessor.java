package com.ctrip.hermes.broker.transport.command.processor;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.tuple.Triple;

import com.ctrip.hermes.broker.biz.logger.BrokerFileBizLogger;
import com.ctrip.hermes.broker.queue.MessageQueueManager;
import com.ctrip.hermes.core.bo.AckContext;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.log.BizEvent;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.transport.command.AckMessageCommand;
import com.ctrip.hermes.core.transport.command.Command;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;
import com.ctrip.hermes.core.transport.command.processor.ThreadCount;
import com.ctrip.hermes.core.transport.command.v2.AckMessageCommandV2;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@ThreadCount(value = 10)
public class AckMessageCommandProcessor implements CommandProcessor {
	private static final Logger log = LoggerFactory.getLogger(AckMessageCommandProcessor.class);

	@Inject
	private BrokerFileBizLogger m_bizLogger;

	@Inject
	private MessageQueueManager m_messageQueueManager;

	@Inject
	private MetaService m_metaService;

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.MESSAGE_ACK, CommandType.MESSAGE_ACK_V2);
	}

	@Override
	public void process(CommandProcessorContext ctx) {
		Command cmd = ctx.getCommand();

		int ackType = AckMessageCommandV2.NORMAL;

		Map<Triple<Tpp, String, Boolean>, List<AckContext>> ackMsgs = null;
		Map<Triple<Tpp, String, Boolean>, List<AckContext>> nackMsgs = null;

		switch (cmd.getHeader().getVersion()) {
		case 1:
			ackMsgs = ((AckMessageCommand) cmd).getAckMsgs();
			nackMsgs = ((AckMessageCommand) cmd).getNackMsgs();
			break;
		case 2:
			ackMsgs = ((AckMessageCommandV2) cmd).getAckMsgs();
			nackMsgs = ((AckMessageCommandV2) cmd).getNackMsgs();
			ackType = ((AckMessageCommandV2) cmd).getType();
			break;
		default:
			if (log.isDebugEnabled()) {
				log.debug("Invalid ack message command version: {}" + cmd.getHeader().getVersion());
			}
			return;
		}

		for (Map.Entry<Triple<Tpp, String, Boolean>, List<AckContext>> entry : ackMsgs.entrySet()) {
			Tpp tpp = entry.getKey().getFirst();
			String groupId = entry.getKey().getMiddle();
			boolean isResend = entry.getKey().getLast();
			List<AckContext> ackContexts = entry.getValue();
			m_messageQueueManager.acked(tpp, groupId, isResend, ackContexts, ackType);
			bizLogAcked(tpp, ctx.getRemoteIp(), groupId, ackContexts, isResend, true);

			if (log.isDebugEnabled()) {
				log.debug("Client acked(topic={}, partition={}, pirority={}, groupId={}, isResend={}, contexts={})",
				      tpp.getTopic(), tpp.getPartition(), tpp.isPriority(), groupId, isResend, ackContexts);
			}
		}

		for (Map.Entry<Triple<Tpp, String, Boolean>, List<AckContext>> entry : nackMsgs.entrySet()) {
			Tpp tpp = entry.getKey().getFirst();
			String groupId = entry.getKey().getMiddle();
			boolean isResend = entry.getKey().getLast();
			List<AckContext> nackContexts = entry.getValue();
			m_messageQueueManager.nacked(tpp, groupId, isResend, nackContexts, ackType);
			bizLogAcked(tpp, ctx.getRemoteIp(), groupId, nackContexts, isResend, false);

			if (log.isDebugEnabled()) {
				log.debug("Client nacked(topic={}, partition={}, pirority={}, groupId={}, isResend={}, contexts={})",
				      tpp.getTopic(), tpp.getPartition(), tpp.isPriority(), groupId, isResend, nackContexts);
			}
		}
	}

	private void bizLogAcked(Tpp tpp, String consumerIp, String groupId, List<AckContext> ackContexts, boolean isResend,
	      boolean ack) {
		for (AckContext ctx : ackContexts) {
			BizEvent ackEvent = new BizEvent("Message.Acked");
			addBizData(ackEvent, tpp, consumerIp, groupId, ctx, isResend, ack);
			addConsumerProcessTime(ackEvent, ctx.getOnMessageStartTimeMillis(), ctx.getOnMessageEndTimeMillis());
			m_bizLogger.log(ackEvent);
		}
	}

	private void addConsumerProcessTime(BizEvent event, long startTime, long endTime) {
		event.addData("bizProcessStartTime", new Date(startTime));
		event.addData("bizProcessEndTime", new Date(endTime));
		event.addData("processTime", endTime - startTime);
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
