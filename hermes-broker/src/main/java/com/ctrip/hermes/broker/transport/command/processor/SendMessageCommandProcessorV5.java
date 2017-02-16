package com.ctrip.hermes.broker.transport.command.processor;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.broker.biz.logger.BrokerFileBizLogger;
import com.ctrip.hermes.broker.lease.BrokerLeaseContainer;
import com.ctrip.hermes.broker.queue.MessageQueueManager;
import com.ctrip.hermes.broker.status.BrokerStatusMonitor;
import com.ctrip.hermes.core.bo.SendMessageResult;
import com.ctrip.hermes.core.constants.CatConstants;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.log.BizEvent;
import com.ctrip.hermes.core.message.PartialDecodedMessage;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.core.transport.ChannelUtils;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.MessageBatchWithRawData;
import com.ctrip.hermes.core.transport.command.SendMessageResultCommand;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;
import com.ctrip.hermes.core.transport.command.v5.SendMessageAckCommandV5;
import com.ctrip.hermes.core.transport.command.v5.SendMessageCommandV5;
import com.ctrip.hermes.core.utils.CatUtil;
import com.ctrip.hermes.env.config.broker.BrokerConfigProvider;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Storage;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Event;
import com.dianping.cat.message.Transaction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.RateLimiter;

/**
 * 
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class SendMessageCommandProcessorV5 implements CommandProcessor {
	private static final Logger log = LoggerFactory.getLogger(SendMessageCommandProcessorV5.class);

	@Inject
	private BrokerFileBizLogger m_bizLogger;

	@Inject
	private MessageQueueManager m_queueManager;

	@Inject
	private BrokerLeaseContainer m_leaseContainer;

	@Inject
	private BrokerConfigProvider m_config;

	@Inject
	private MetaService m_metaService;

	@Inject
	private SystemClockService m_systemClockService;

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.MESSAGE_SEND_V5);
	}

	@Override
	public void process(final CommandProcessorContext ctx) {
		SendMessageCommandV5 reqCmd = (SendMessageCommandV5) ctx.getCommand();
		String topic = reqCmd.getTopic();
		int partition = reqCmd.getPartition();

		logReqToCat(reqCmd);

		Lease lease = m_leaseContainer.acquireLease(topic, partition, m_config.getSessionId());

		if (m_metaService.findTopicByName(topic) != null) {
			if (lease != null) {
				int bytes = calSize(reqCmd.getMessageRawDataBatches());

				if (!isRateLimitExceeded(topic, partition, reqCmd.getMessageCount(), bytes)) {
					writeAck(ctx, topic, partition, true);

					Map<Integer, MessageBatchWithRawData> rawBatches = reqCmd.getMessageRawDataBatches();

					bizLog(ctx, rawBatches, partition);

					final SendMessageResultCommand result = new SendMessageResultCommand(reqCmd.getMessageCount());
					result.correlate(reqCmd);

					FutureCallback<Map<Integer, SendMessageResult>> completionCallback = new AppendMessageCompletionCallback(
					      result, ctx, topic, partition);

					for (Map.Entry<Integer, MessageBatchWithRawData> entry : rawBatches.entrySet()) {
						MessageBatchWithRawData batch = entry.getValue();
						try {
							ListenableFuture<Map<Integer, SendMessageResult>> future = m_queueManager.appendMessageAsync(
							      topic, partition, entry.getKey() == 0 ? true : false, batch, m_systemClockService.now()
							            + reqCmd.getTimeout());

							if (future != null) {
								Futures.addCallback(future, completionCallback);
							}
						} catch (Exception e) {
							log.error("Failed to append messages async.", e);
						}
					}
				} else {
					reqCmd.release();
				}
			} else {
				if (log.isDebugEnabled()) {
					log.debug("No broker lease to handle client send message reqeust(topic={}, partition={})", topic,
					      partition);
				}
				writeAck(ctx, topic, partition, false);
				reqCmd.release();
			}
		} else {
			if (log.isDebugEnabled()) {
				log.debug("Topic {} not found", topic);
			}
			writeAck(ctx, topic, partition, false);
			reqCmd.release();
		}

	}

	private boolean isRateLimitExceeded(String topic, int partition, int msgCount, int bytes) {
		RateLimiter qpsRateLimiter = m_config.getPartitionProduceQPSRateLimiter(topic, partition);
		RateLimiter bytesRateLimiter = m_config.getPartitionProduceBytesRateLimiter(topic, partition);
		if (qpsRateLimiter.tryAcquire(msgCount)) {
			if (bytesRateLimiter.tryAcquire(bytes)) {
				return false;
			} else {
				Cat.logEvent(CatConstants.TYPE_MESSAGE_BROKER_BYTES_RATE_LIMIT_EXCEED, topic + "-" + partition,
				      Event.SUCCESS, "msgCount=" + msgCount + "&bytes=" + bytes);
			}
		} else {
			Cat.logEvent(CatConstants.TYPE_MESSAGE_BROKER_QPS_RATE_LIMIT_EXCEED, topic + "-" + partition, Event.SUCCESS,
			      "msgCount=" + msgCount + "&bytes=" + bytes);
		}

		return true;
	}

	private int calSize(Map<Integer, MessageBatchWithRawData> messageRawDataBatches) {
		int bytes = 0;

		for (MessageBatchWithRawData batch : messageRawDataBatches.values()) {
			for (PartialDecodedMessage pdmsg : batch.getMessages()) {
				bytes += pdmsg.getDurableProperties().readableBytes() + pdmsg.getBody().readableBytes();
			}
		}

		return bytes;
	}

	private void logReqToCat(SendMessageCommandV5 reqCmd) {
		CatUtil.logEventPeriodically(CatConstants.TYPE_SEND_CMD + reqCmd.getHeader().getType().getVersion(),
		      reqCmd.getTopic() + "-" + reqCmd.getPartition());
	}

	private void bizLog(CommandProcessorContext ctx, Map<Integer, MessageBatchWithRawData> rawBatches, int partition) {
		for (Entry<Integer, MessageBatchWithRawData> entry : rawBatches.entrySet()) {
			MessageBatchWithRawData batch = entry.getValue();
			if (!Storage.KAFKA.equals(m_metaService.findTopicByName(batch.getTopic()).getStorageType())) {
				List<PartialDecodedMessage> msgs = batch.getMessages();
				BrokerStatusMonitor.INSTANCE.msgReceived(batch.getTopic(), partition, ctx.getRemoteIp(), batch.getRawData()
				      .readableBytes(), msgs.size());
				for (PartialDecodedMessage msg : msgs) {
					BizEvent event = new BizEvent("Message.Received");
					event.addData("topic", m_metaService.findTopicByName(batch.getTopic()).getId());
					event.addData("partition", partition);
					event.addData("priority", entry.getKey());
					event.addData("producerIp", ctx.getRemoteIp());
					event.addData("bornTime", new Date(msg.getBornTime()));
					event.addData("refKey", msg.getKey());

					m_bizLogger.log(event);
				}
			}
		}
	}

	private class AppendMessageCompletionCallback implements FutureCallback<Map<Integer, SendMessageResult>> {
		private SendMessageResultCommand m_result;

		private CommandProcessorContext m_ctx;

		private AtomicBoolean m_written = new AtomicBoolean(false);

		private String m_topic;

		private int m_partition;

		private long m_start;

		private boolean m_shouldResponse;

		public AppendMessageCompletionCallback(SendMessageResultCommand result, CommandProcessorContext ctx,
		      String topic, int partition) {
			m_result = result;
			m_ctx = ctx;
			m_topic = topic;
			m_partition = partition;
			m_start = System.currentTimeMillis();
			m_shouldResponse = true;
		}

		@Override
		public void onSuccess(Map<Integer, SendMessageResult> results) {
			m_result.addResults(convertResults(results));
			if (m_shouldResponse) {
				for (SendMessageResult sendMessageResult : results.values()) {
					if (!sendMessageResult.isShouldResponse()) {
						m_shouldResponse = false;
						break;
					}
				}
			}

			if (m_result.isAllResultsSet()) {
				try {
					if (m_written.compareAndSet(false, true)) {
						logElapse(m_result);
						if (m_shouldResponse) {
							ChannelUtils.writeAndFlush(m_ctx.getChannel(), m_result);
						}
					}
				} finally {
					m_ctx.getCommand().release();
				}
			}
		}

		private Map<Integer, Boolean> convertResults(Map<Integer, SendMessageResult> results) {
			Map<Integer, Boolean> newResults = new HashMap<>();

			for (Map.Entry<Integer, SendMessageResult> entry : results.entrySet()) {
				newResults.put(entry.getKey(), entry.getValue().isSuccess() ? true : entry.getValue().isShouldSkip());
			}

			return newResults;
		}

		private void logElapse(SendMessageResultCommand resultCmd) {
			int failCount = 0;
			for (Boolean sendSuccess : resultCmd.getSuccesses().values()) {
				if (!sendSuccess) {
					failCount++;
				}
			}

			int successCount = m_result.getSuccesses().size() - failCount;

			if (successCount > 0) {
				CatUtil.logElapse(CatConstants.TYPE_MESSAGE_BROKER_PRODUCE_DB + findDb(m_topic, m_partition), m_topic,
				      m_start, successCount, null, Transaction.SUCCESS);
				CatUtil.logElapse(CatConstants.TYPE_MESSAGE_BROKER_PRODUCE, m_topic, m_start, successCount, null,
				      Transaction.SUCCESS);
			}

			if (failCount > 0) {
				CatUtil.logElapse(CatConstants.TYPE_MESSAGE_BROKER_PRODUCE_DB + findDb(m_topic, m_partition), m_topic,
				      m_start, failCount, null, CatConstants.TRANSACTION_FAIL);
				CatUtil.logElapse(CatConstants.TYPE_MESSAGE_BROKER_PRODUCE, m_topic, m_start, failCount, null,
				      CatConstants.TRANSACTION_FAIL);
			}
		}

		private String findDb(String topic, int partition) {
			Partition p = m_metaService.findPartitionByTopicAndPartition(topic, partition);
			return p.getWriteDatasource();
		}

		@Override
		public void onFailure(Throwable t) {
			// TODO
		}
	}

	private void writeAck(CommandProcessorContext ctx, String topic, int partition, boolean success) {
		SendMessageCommandV5 req = (SendMessageCommandV5) ctx.getCommand();

		SendMessageAckCommandV5 ack = new SendMessageAckCommandV5();
		ack.correlate(req);
		ack.setSuccess(success);
		if (!success && m_metaService.findTopicByName(topic) != null) {
			Pair<Endpoint, Long> endpointEntry = m_metaService.findEndpointByTopicAndPartition(topic, partition);
			if (endpointEntry != null) {
				ack.setNewEndpoint(endpointEntry.getKey());
			}
		}
		ChannelUtils.writeAndFlush(ctx.getChannel(), ack);
	}

}
