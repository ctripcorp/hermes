package com.ctrip.hermes.broker.longpolling;

import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.broker.biz.logger.BrokerFileBizLogger;
import com.ctrip.hermes.broker.queue.MessageQueueCursor;
import com.ctrip.hermes.broker.selector.PullMessageSelectorManager;
import com.ctrip.hermes.broker.status.BrokerStatusMonitor;
import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.bo.Tp;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.constants.CatConstants;
import com.ctrip.hermes.core.log.BizEvent;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch.DummyMessageMeta;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch.MessageMeta;
import com.ctrip.hermes.core.selector.CallbackContext;
import com.ctrip.hermes.core.selector.ExpireTimeHolder;
import com.ctrip.hermes.core.selector.SelectorCallback;
import com.ctrip.hermes.core.selector.TriggerResult;
import com.ctrip.hermes.core.selector.TriggerResult.State;
import com.ctrip.hermes.core.utils.CatUtil;
import com.ctrip.hermes.meta.entity.Partition;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = LongPollingService.class)
public class DefaultLongPollingService extends AbstractLongPollingService implements Initializable {

	@Inject
	private BrokerFileBizLogger m_bizLogger;

	@Inject
	private PullMessageSelectorManager selectorManager;

	private static final Logger log = LoggerFactory.getLogger(DefaultLongPollingService.class);

	private TreeMap<Integer, String> m_catSelectorByPriorityMetrics = new TreeMap<>();

	private TreeMap<Integer, String> m_catSelectorByNonPriorityMetrics = new TreeMap<>();

	private TreeMap<Integer, String> m_catSelectorBySafeTriggerMetrics = new TreeMap<>();

	@Override
	public void initialize() throws InitializationException {
		m_catSelectorByPriorityMetrics.put(0, CatConstants.TYPE_MESSAGE_DELIVER_BY_PRIORITY + "0");
		m_catSelectorByPriorityMetrics.put(1, CatConstants.TYPE_MESSAGE_DELIVER_BY_PRIORITY + "1");
		m_catSelectorByPriorityMetrics.put(10, CatConstants.TYPE_MESSAGE_DELIVER_BY_PRIORITY + "2-10");
		m_catSelectorByPriorityMetrics.put(50, CatConstants.TYPE_MESSAGE_DELIVER_BY_PRIORITY + "11-50");
		m_catSelectorByPriorityMetrics.put(Integer.MAX_VALUE, CatConstants.TYPE_MESSAGE_DELIVER_BY_PRIORITY + "gt-50");

		m_catSelectorByNonPriorityMetrics.put(0, CatConstants.TYPE_MESSAGE_DELIVER_BY_NONPRIORITY + "0");
		m_catSelectorByNonPriorityMetrics.put(1, CatConstants.TYPE_MESSAGE_DELIVER_BY_NONPRIORITY + "1");
		m_catSelectorByNonPriorityMetrics.put(10, CatConstants.TYPE_MESSAGE_DELIVER_BY_NONPRIORITY + "2-10");
		m_catSelectorByNonPriorityMetrics.put(50, CatConstants.TYPE_MESSAGE_DELIVER_BY_NONPRIORITY + "11-50");
		m_catSelectorByNonPriorityMetrics.put(Integer.MAX_VALUE, CatConstants.TYPE_MESSAGE_DELIVER_BY_NONPRIORITY
		      + "gt-50");

		m_catSelectorBySafeTriggerMetrics.put(0, CatConstants.TYPE_MESSAGE_DELIVER_BY_SAFE + "0");
		m_catSelectorBySafeTriggerMetrics.put(1, CatConstants.TYPE_MESSAGE_DELIVER_BY_SAFE + "1");
		m_catSelectorBySafeTriggerMetrics.put(10, CatConstants.TYPE_MESSAGE_DELIVER_BY_SAFE + "2-10");
		m_catSelectorBySafeTriggerMetrics.put(50, CatConstants.TYPE_MESSAGE_DELIVER_BY_SAFE + "11-50");
		m_catSelectorBySafeTriggerMetrics.put(Integer.MAX_VALUE, CatConstants.TYPE_MESSAGE_DELIVER_BY_SAFE + "gt-50");

	}

	@Override
	public void schedulePush(final PullMessageTask task) {
		Tpg tpg = task.getTpg();
		if (log.isDebugEnabled()) {
			log.debug("Schedule push(correlation id: {}) for client: {}", task.getCorrelationId(), tpg);
		}

		if (m_stopped.get() || (task.isWithOffset() && task.getStartOffset() == null)) {
			response(task, null, null, true);
		} else {
			Tp tp = new Tp(tpg.getTopic(), tpg.getPartition());
			Offset startOffsets = task.getStartOffset();
			selectorManager.register(tp, new SelectorTaskExpireTimeHoler(task), new SelectorCallback() {

				@Override
				public void onReady(CallbackContext ctx) {
					executeTask(task, ctx);
				}
			}, tpg.getGroupId(), startOffsets.getPriorityOffset(), startOffsets.getNonPriorityOffset());
		}
	}

	private void executeTask(final PullMessageTask pullMessageTask, CallbackContext callbackCtx) {
		if (m_stopped.get()) {
			return;
		}
		try {
			// skip expired task
			Tpg tpg = pullMessageTask.getTpg();
			if (pullMessageTask.getExpireTime() < m_systemClockService.now()) {
				if (log.isDebugEnabled()) {
					log.debug("Client expired(correlationId={}, topic={}, partition={}, groupId={})",
					      pullMessageTask.getCorrelationId(), tpg.getTopic(), tpg.getPartition(), tpg.getGroupId());
				}
				return;
			}

			if (!pullMessageTask.getBrokerLease().isExpired()) {
				Pair<Boolean, Boolean> queryAndResponseResult = queryAndResponseData(pullMessageTask, callbackCtx);
				boolean someDataResponsed = queryAndResponseResult.getKey();
				boolean someErrorOccurred = queryAndResponseResult.getValue();

				if (!someDataResponsed) {
					if (!m_stopped.get()) {
						final Tp tp = new Tp(tpg.getTopic(), tpg.getPartition());
						Offset startOffsets = pullMessageTask.getStartOffset();

						State state;
						if (someErrorOccurred) {
							state = State.GotButErrorInProcessing;
						} else {
							state = State.GotNothing;
						}

						TriggerResult triggerResult = new TriggerResult(state, new long[] { startOffsets.getPriorityOffset(),
						      startOffsets.getNonPriorityOffset() }, tpg.getGroupId());

						selectorManager.reRegister(tp, callbackCtx, triggerResult, new SelectorTaskExpireTimeHoler(
						      pullMessageTask), new SelectorCallback() {

							@Override
							public void onReady(CallbackContext innerCtx) {
								executeTask(pullMessageTask, innerCtx);
							}
						});
					}
				}
			} else {
				if (log.isDebugEnabled()) {
					log.debug("Broker no lease for this request(correlationId={}, topic={}, partition={}, groupId={})",
					      pullMessageTask.getCorrelationId(), tpg.getTopic(), tpg.getPartition(), tpg.getGroupId());
				}
				// no lease, return empty cmd
				response(pullMessageTask, null, null, false);
			}
		} catch (Exception e) {
			log.error("Exception occurred while executing pull message task", e);
		}
	}

	private Pair<Boolean, Boolean> queryAndResponseData(PullMessageTask pullTask, CallbackContext callbackCtx) {
		boolean someDataResponsed = false;
		boolean someErrorOccurred = false;

		long startTime = System.currentTimeMillis();
		Tpg tpg = pullTask.getTpg();

		MessageQueueCursor cursor = m_queueManager.getCursor(tpg, pullTask.getBrokerLease(), pullTask.getStartOffset());

		if (cursor == null) {
			someErrorOccurred = true;
			return new Pair<>(someDataResponsed, someErrorOccurred);
		}

		Pair<Offset, List<TppConsumerMessageBatch>> p = null;

		try {
			p = cursor.next(pullTask.getBatchSize(), pullTask.getFilter(), callbackCtx);
		} finally {
			cursor.stop();
		}

		if (p != null) {
			Offset currentOffset = p.getKey();
			List<TppConsumerMessageBatch> batches = p.getValue();

			if (batches != null && !batches.isEmpty()) {

				boolean responseOk = response(pullTask, batches, currentOffset, true);

				int count = 0;
				for (TppConsumerMessageBatch batch : batches) {
					// TODO remove legacy code
					boolean needServerSideAckHolder = pullTask.getPullMessageCommandVersion() < 3 ? true : false;
					m_queueManager.delivered(batch, tpg.getGroupId(), pullTask.isWithOffset(), needServerSideAckHolder);

					bizLogDelivered(pullTask.getClientIp(), batch.getMessageMetas(), tpg, pullTask.getReceiveTime(),
					      pullTask.getPullTime(), responseOk);

					count += batch.size();
				}

				CatUtil.logElapse(CatConstants.TYPE_MESSAGE_DELIVER_DB + findDb(tpg.getTopic(), tpg.getPartition()),
				      tpg.getTopic(), startTime, count, null, Transaction.SUCCESS);
				CatUtil.logElapse(CatConstants.TYPE_MESSAGE_DELIVER, tpg.getTopic(), startTime, count, null,
				      Transaction.SUCCESS);

				logSelecotrMetric(pullTask.getTpg(), callbackCtx, count);

				someDataResponsed = true;
				return new Pair<>(someDataResponsed, someErrorOccurred);
			} else {
				logSelecotrMetric(pullTask.getTpg(), callbackCtx, 0);
				return new Pair<>(someDataResponsed, someErrorOccurred);
			}
		} else {
			someErrorOccurred = true;
			return new Pair<>(someDataResponsed, someErrorOccurred);
		}
	}

	private String findDb(String topic, int partition) {
		Partition p = m_metaService.findPartitionByTopicAndPartition(topic, partition);
		return p.getReadDatasource();
	}

	private void logSelecotrMetric(Tpg tpg, CallbackContext callbackCtx, int count) {
		TreeMap<Integer, String> metricNames = null;

		if (callbackCtx.getSlotMatchResults()[PullMessageSelectorManager.SLOT_PRIORITY_INDEX].isMatch()) {
			metricNames = m_catSelectorByPriorityMetrics;
		} else if (callbackCtx.getSlotMatchResults()[PullMessageSelectorManager.SLOT_NONPRIORITY_INDEX].isMatch()) {
			metricNames = m_catSelectorByNonPriorityMetrics;
		} else {
			metricNames = m_catSelectorBySafeTriggerMetrics;
		}

		Entry<Integer, String> ceilingEntry = metricNames.ceilingEntry(count);

		Cat.logEvent(ceilingEntry.getValue(), tpg.getTopic() + "-" + tpg.getPartition());
	}

	private void bizLogDelivered(String ip, List<MessageMeta> metas, Tpg tpg, Date pullCmdReceiveTime, Date pullTime,
	      boolean networkWritten) {
		BrokerStatusMonitor.INSTANCE.msgDelivered(tpg.getTopic(), tpg.getPartition(), tpg.getGroupId(), ip, metas.size());

		for (MessageMeta meta : metas) {
			if (!(meta instanceof DummyMessageMeta)) {
				BizEvent event = new BizEvent("Message.Delivered");
				event.addData("msgId", meta.getOriginId());
				event.addData("topic", m_metaService.findTopicByName(tpg.getTopic()).getId());
				event.addData("partition", tpg.getPartition());
				event.addData("consumerIp", ip);
				event.addData("groupId", m_metaService.translateToIntGroupId(tpg.getTopic(), tpg.getGroupId()));
				event.addData("pullCmdReceiveTime", pullCmdReceiveTime);
				event.addData("networkWritten", networkWritten);
				event.addData("isResend", meta.isResend());
				event.addData("priority", meta.getPriority());
				if (pullTime != null) {
					event.addData("pullTime", pullTime);
				}
				if (meta.isResend()) {
					event.addData("resendId", meta.getId());
				}

				m_bizLogger.log(event);
			}
		}
	}

	@Override
	protected void doStop() {
	}

	private static class SelectorTaskExpireTimeHoler implements ExpireTimeHolder {

		private PullMessageTask task;

		public SelectorTaskExpireTimeHoler(PullMessageTask task) {
			this.task = task;
		}

		@Override
		public long currentExpireTime() {
			return Math.min(task.getBrokerLease().getExpireTime(), task.getExpireTime());
		}

	}
}
