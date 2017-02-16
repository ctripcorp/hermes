package com.ctrip.hermes.producer.monitor;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.bo.SendMessageResult;
import com.ctrip.hermes.core.constants.CatConstants;
import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.transport.command.v6.SendMessageCommandV6;
import com.ctrip.hermes.core.transport.command.v6.SendMessageResultCommandV6;
import com.ctrip.hermes.core.utils.CatUtil;
import com.ctrip.hermes.producer.config.ProducerConfig;
import com.ctrip.hermes.producer.status.ProducerStatusMonitor;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;
import com.dianping.cat.message.spi.MessageTree;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = SendMessageResultMonitor.class)
public class DefaultSendMessageResultMonitor implements SendMessageResultMonitor {
	private static final Logger log = LoggerFactory.getLogger(DefaultSendMessageResultMonitor.class);

	private Map<Long, Pair<SendMessageCommandV6, SettableFuture<SendMessageResult>>> m_cmds = new ConcurrentHashMap<>();

	private ReentrantLock m_lock = new ReentrantLock();

	@Inject
	private ProducerConfig m_config;

	@Override
	public Future<SendMessageResult> monitor(SendMessageCommandV6 cmd) {
		m_lock.lock();
		try {
			SettableFuture<SendMessageResult> future = SettableFuture.create();
			m_cmds.put(cmd.getHeader().getCorrelationId(),
			      new Pair<SendMessageCommandV6, SettableFuture<SendMessageResult>>(cmd, future));
			return future;
		} finally {
			m_lock.unlock();
		}
	}

	@Override
	public void resultReceived(SendMessageResultCommandV6 resultCmd) {
		if (resultCmd != null) {
			Pair<SendMessageCommandV6, SettableFuture<SendMessageResult>> pair = null;
			m_lock.lock();
			try {
				pair = m_cmds.remove(resultCmd.getHeader().getCorrelationId());
			} finally {
				m_lock.unlock();
			}
			if (pair != null) {
				try {
					SendMessageCommandV6 sendMessageCommand = pair.getKey();
					SettableFuture<SendMessageResult> future = pair.getValue();

					SendMessageResult sendMessageResult = convertToSingleSendMessageResult(resultCmd);

					if (sendMessageResult.isSuccess()) {
						future.set(sendMessageResult);
						sendMessageCommand.onResultReceived(resultCmd);
						tracking(sendMessageCommand);
					} else {
						if (sendMessageResult.isShouldSkip()) {
							ProducerStatusMonitor.INSTANCE.sendSkip(sendMessageCommand.getTopic(),
							      sendMessageCommand.getPartition(), sendMessageCommand.getMessageCount());
							sendMessageCommand.onResultReceived(resultCmd);
						} else {
							ProducerStatusMonitor.INSTANCE.sendFail(sendMessageCommand.getTopic(),
							      sendMessageCommand.getPartition(), sendMessageCommand.getMessageCount());
						}
						future.set(sendMessageResult);
					}

				} catch (Exception e) {
					log.warn("Exception occurred while calling resultReceived", e);
				}

			}
		}
	}

	private SendMessageResult convertToSingleSendMessageResult(SendMessageResultCommandV6 cmd) {

		Map<Integer, SendMessageResult> results = cmd.getResults();
		boolean success = true;
		boolean shouldSkip = false;
		String errorMsg = null;
		for (SendMessageResult res : results.values()) {
			if (!res.isSuccess()) {
				success = false;
				if (errorMsg == null && res.getErrorMessage() != null) {
					errorMsg = res.getErrorMessage();
				}

				if (res.isShouldSkip()) {
					shouldSkip = true;
					if (res.getErrorMessage() != null) {
						errorMsg = res.getErrorMessage();
					}
				}
			}
		}

		return new SendMessageResult(success, shouldSkip, errorMsg);
	}

	private void tracking(SendMessageCommandV6 sendMessageCommand) {
		if (m_config.isCatEnabled()) {
			Transaction t = Cat.newTransaction(CatConstants.TYPE_MESSAGE_PRODUCE_ACKED, sendMessageCommand.getTopic());
			for (List<ProducerMessage<?>> msgs : sendMessageCommand.getProducerMessages()) {
				for (ProducerMessage<?> msg : msgs) {
					MessageTree tree = Cat.getManager().getThreadLocalMessageTree();

					String msgId = msg.getDurableSysProperty(CatConstants.SERVER_MESSAGE_ID);
					String parentMsgId = msg.getDurableSysProperty(CatConstants.CURRENT_MESSAGE_ID);
					String rootMsgId = msg.getDurableSysProperty(CatConstants.ROOT_MESSAGE_ID);

					tree.setMessageId(msgId);
					tree.setParentMessageId(parentMsgId);
					tree.setRootMessageId(rootMsgId);

					long duration = System.currentTimeMillis() - msg.getBornTime();
					String type = duration > 2000L ? CatConstants.TYPE_MESSAGE_PRODUCE_ELAPSE_LARGE
					      : CatConstants.TYPE_MESSAGE_PRODUCE_ELAPSE;
					CatUtil.logElapse(type, msg.getTopic(), msg.getBornTime(), 1,
					      Arrays.asList(new Pair<String, String>("key", msg.getKey())), Transaction.SUCCESS);
					ProducerStatusMonitor.INSTANCE.sendSuccess(msg.getTopic(), msg.getPartition(), duration);
				}
			}
			t.addData("*count", sendMessageCommand.getMessageCount());
			t.setStatus(Transaction.SUCCESS);
			t.complete();
		}
	}

	@Override
	public void cancel(SendMessageCommandV6 cmd) {
		m_lock.lock();
		try {
			m_cmds.remove(cmd.getHeader().getCorrelationId());
		} finally {
			m_lock.unlock();
		}
	}

}
