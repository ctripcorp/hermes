package com.ctrip.hermes.broker.queue.storage.mysql;

import static com.ctrip.hermes.broker.dal.hermes.MessagePriorityEntity.READSET_OFFSET;

import java.io.ByteArrayInputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.codehaus.plexus.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.helper.Files.IO;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;
import org.unidal.tuple.Triple;

import com.ctrip.hermes.broker.biz.logger.BrokerFileBizLogger;
import com.ctrip.hermes.broker.dal.hermes.DeadLetter;
import com.ctrip.hermes.broker.dal.hermes.DeadLetterDao;
import com.ctrip.hermes.broker.dal.hermes.MessagePriority;
import com.ctrip.hermes.broker.dal.hermes.MessagePriorityDao;
import com.ctrip.hermes.broker.dal.hermes.MessagePriorityEntity;
import com.ctrip.hermes.broker.dal.hermes.OffsetMessage;
import com.ctrip.hermes.broker.dal.hermes.OffsetMessageDao;
import com.ctrip.hermes.broker.dal.hermes.OffsetMessageEntity;
import com.ctrip.hermes.broker.dal.hermes.OffsetResend;
import com.ctrip.hermes.broker.dal.hermes.OffsetResendDao;
import com.ctrip.hermes.broker.dal.hermes.OffsetResendEntity;
import com.ctrip.hermes.broker.dal.hermes.ResendGroupId;
import com.ctrip.hermes.broker.dal.hermes.ResendGroupIdDao;
import com.ctrip.hermes.broker.dal.hermes.ResendGroupIdEntity;
import com.ctrip.hermes.broker.queue.storage.FetchResult;
import com.ctrip.hermes.broker.queue.storage.MessageQueueStorage;
import com.ctrip.hermes.broker.queue.storage.filter.Filter;
import com.ctrip.hermes.broker.queue.storage.mysql.ack.MySQLMessageAckFlusher;
import com.ctrip.hermes.broker.queue.storage.mysql.dal.CachedMessagePriorityDaoInterceptor;
import com.ctrip.hermes.broker.selector.PullMessageSelectorManager;
import com.ctrip.hermes.broker.status.BrokerStatusMonitor;
import com.ctrip.hermes.broker.transport.BrokerDummyMessagePriority;
import com.ctrip.hermes.core.bo.Tp;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.constants.CatConstants;
import com.ctrip.hermes.core.log.BizEvent;
import com.ctrip.hermes.core.message.PartialDecodedMessage;
import com.ctrip.hermes.core.message.PropertiesHolder;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch.DummyMessageMeta;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch.MessageMeta;
import com.ctrip.hermes.core.message.codec.MessageCodec;
import com.ctrip.hermes.core.message.retry.RetryPolicy;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.selector.Slot;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.core.transport.TransferCallback;
import com.ctrip.hermes.core.transport.command.MessageBatchWithRawData;
import com.ctrip.hermes.core.utils.CatUtil;
import com.ctrip.hermes.core.utils.CollectionUtil;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;
import com.ctrip.hermes.env.config.broker.BrokerConfigProvider;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;
import com.dianping.cat.Cat;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = MessageQueueStorage.class, value = Storage.MYSQL)
public class MySQLMessageQueueStorage implements MessageQueueStorage, Initializable {
	private static final Logger log = LoggerFactory.getLogger(MySQLMessageQueueStorage.class);

	@Inject
	private BrokerFileBizLogger m_bizLogger;

	@Inject
	private MessageCodec m_messageCodec;

	@Inject
	private MessagePriorityDao m_msgDao;

	@Inject
	private ResendGroupIdDao m_resendDao;

	@Inject
	private OffsetResendDao m_offsetResendDao;

	@Inject
	private OffsetMessageDao m_offsetMessageDao;

	@Inject
	private DeadLetterDao m_deadLetterDao;

	@Inject
	private MetaService m_metaService;

	@Inject
	private SystemClockService m_systemClockService;

	@Inject
	private BrokerConfigProvider m_config;

	@Inject
	private Filter m_filter;

	@Inject
	private PullMessageSelectorManager selectorManager;

	@Inject
	private MySQLMessageAckFlusher m_ackFlusher;

	private Map<Triple<String, Integer, Integer>, OffsetResend> m_offsetResendCache = new ConcurrentHashMap<>();

	private Map<Pair<Tpp, Integer>, OffsetMessage> m_offsetMessageCache = new ConcurrentHashMap<>();

	private TreeMap<Integer, String> m_catSelectorByPriorityMetrics = new TreeMap<>();

	private TreeMap<Integer, String> m_catSelectorByNonPriorityMetrics = new TreeMap<>();

	private Field m_bufFieldOfByteArrayInputStream;

	@Override
	public void appendMessages(String topicName, int partition, boolean priority,
	      Collection<MessageBatchWithRawData> batches) throws Exception {
		List<MessagePriority> msgs = new LinkedList<>();

		Topic topic = m_metaService.findTopicByName(topicName);

		int count = 0;
		long bytes = 0;

		for (MessageBatchWithRawData batch : batches) {
			List<PartialDecodedMessage> pdmsgs = batch.getMessages();
			BrokerStatusMonitor.INSTANCE.msgSaved(topicName, partition, pdmsgs.size());
			for (PartialDecodedMessage pdmsg : pdmsgs) {
				count++;
				bytes += pdmsg.getBody().readableBytes() + pdmsg.getDurableProperties().readableBytes();
				MessagePriority msg = new MessagePriority();
				msg.setAttributes(pdmsg.readDurableProperties());
				msg.setCreationDate(new Date(pdmsg.getBornTime()));
				msg.setPartition(partition);
				msg.setPayload(new ByteBufInputStream(pdmsg.getBody().duplicate()));
				if (topic.isPriorityMessageEnabled()) {
					msg.setPriority(priority ? 0 : 1);
				} else {
					msg.setPriority(1);
				}
				// TODO set producer id and producer id in producer
				msg.setProducerId(0);
				msg.setProducerIp("");
				msg.setRefKey(pdmsg.getKey());
				msg.setTopic(topicName);
				msg.setCodecType(pdmsg.getBodyCodecType());

				msgs.add(msg);
			}
		}

		if (!msgs.isEmpty()) {
			batchInsert(topic, partition, priority, msgs);
		}

		if (count > 0) {
			logSelecotrMetric(topicName, partition, priority, count);
		}

		if (bytes > 0) {
			try {
				CatUtil.logEventPeriodically(
				      CatConstants.TYPE_MESSAGE_BROKER_PRODUCE_BYTES
				            + topic.getPartitions().get(partition).getWriteDatasource(), topicName, bytes);
			} catch (Exception e) {
				log.warn("Exception occurred while loging bytes for {}-{}", topicName, partition, e);
			}
		}
	}

	private void batchInsert(Topic topic, int partition, boolean priority, List<MessagePriority> msgs)
	      throws DalException {
		long startTime = m_systemClockService.now();
		m_msgDao.insert(msgs.toArray(new MessagePriority[msgs.size()]));
		notifySelector(topic, partition, priority, msgs);

		bizLog(topic.getName(), partition, priority, msgs, startTime, m_systemClockService.now());
	}

	private void logSelecotrMetric(String topic, int partition, boolean priority, int count) {
		TreeMap<Integer, String> metricNames = priority ? m_catSelectorByPriorityMetrics
		      : m_catSelectorByNonPriorityMetrics;

		Entry<Integer, String> ceilingEntry = metricNames.ceilingEntry(count);

		Cat.logEvent(ceilingEntry.getValue(), topic + "-" + partition);
	}

	private void notifySelector(Topic topic, int partition, boolean priority, List<MessagePriority> msgs) {
		if (msgs != null && msgs.size() > 0) {
			long maxId = -1L;
			for (MessagePriority msg : msgs) {
				maxId = Math.max(maxId, msg.getId());
			}

			if (maxId > 0) {
				boolean insertToPriorityTable = topic.isPriorityMessageEnabled() && priority;

				int slotIndex = insertToPriorityTable ? PullMessageSelectorManager.SLOT_PRIORITY_INDEX
				      : PullMessageSelectorManager.SLOT_NONPRIORITY_INDEX;
				Slot slot = new Slot(slotIndex, maxId);
				selectorManager.getSelector().update(new Tp(topic.getName(), partition), true, slot);
			}
		}
	}

	private void bizLog(String topic, int partition, boolean priority, List<MessagePriority> msgs, long startTime,
	      long endTime) {
		for (MessagePriority msg : msgs) {
			BizEvent event = new BizEvent("RefKey.Transformed");
			event.addData("topic", m_metaService.findTopicByName(topic).getId());
			event.addData("partition", partition);
			event.addData("priority", priority ? 0 : 1);
			event.addData("refKey", msg.getRefKey());
			event.addData("msgId", msg.getId());

			m_bizLogger.log(event);
		}
	}

	@Override
	public synchronized Object findLastOffset(Tpp tpp, int groupId) throws DalException {
		String topic = tpp.getTopic();
		int partition = tpp.getPartition();
		int priority = tpp.getPriorityInt();

		if (!hasStorageForPriority(tpp.getTopic(), tpp.getPriorityInt())) {
			return 0L;
		}

		return findLastOffset(topic, partition, priority, groupId).getOffset();
	}

	private synchronized OffsetMessage findLastOffset(String topic, int partition, int priority, int intGroupId)
	      throws DalException {
		List<OffsetMessage> lastOffset = m_offsetMessageDao.find(topic, partition, priority, intGroupId,
		      OffsetMessageEntity.READSET_FULL);

		if (lastOffset.isEmpty()) {
			List<MessagePriority> topMsg = m_msgDao.top(topic, partition, priority, MessagePriorityEntity.READSET_FULL);

			long startOffset = 0L;
			if (!topMsg.isEmpty()) {
				startOffset = CollectionUtil.last(topMsg).getId();
			}

			OffsetMessage offset = new OffsetMessage();
			offset.setCreationDate(new Date());
			offset.setGroupId(intGroupId);
			offset.setOffset(startOffset);
			offset.setPartition(partition);
			offset.setPriority(priority);
			offset.setTopic(topic);

			m_offsetMessageDao.insert(offset);
			return offset;
		} else {
			return CollectionUtil.last(lastOffset);
		}
	}

	private List<Long[]> splitOffsets(List<Object> offsets) {
		int batchSize = m_config.getFetchMessageWithOffsetBatchSize();
		List<Long[]> list = new ArrayList<>();
		for (int idx = 0; idx < offsets.size(); idx += batchSize) {
			List<Object> l = offsets.subList(idx, idx + batchSize < offsets.size() ? idx + batchSize : offsets.size());
			list.add(l.toArray(new Long[l.size()]));
		}
		return list;
	}

	@Override
	public FetchResult fetchMessages(Tpp tpp, List<Object> offsets) {
		List<MessagePriority> msgs = new ArrayList<MessagePriority>();

		if (!hasStorageForPriority(tpp.getTopic(), tpp.getPriorityInt())) {
			return buildFetchResult(tpp, msgs, null);
		}

		for (Long[] subOffsets : splitOffsets(offsets)) {
			try {
				msgs.addAll(m_msgDao.findWithOffsets(tpp.getTopic(), tpp.getPartition(), tpp.getPriorityInt(), subOffsets,
				      MessagePriorityEntity.READSET_FULL));
			} catch (Exception e) {
				log.error("Failed to fetch message({}).", tpp, e);
				continue;
			}
		}
		return buildFetchResult(tpp, msgs, null);
	}

	@Override
	public FetchResult fetchMessages(Tpp tpp, Object startOffset, int batchSize, String filter) {
		if (!hasStorageForPriority(tpp.getTopic(), tpp.getPriorityInt())) {
			return buildFetchResult(tpp, new ArrayList<MessagePriority>(), null);
		}

		try {
			return buildFetchResult(tpp, m_msgDao.findIdAfter(tpp.getTopic(), tpp.getPartition(), tpp.getPriorityInt(),
			      (Long) startOffset, batchSize, MessagePriorityEntity.READSET_FULL), filter);
		} catch (DalException e) {
			log.error("Failed to fetch message({}).", tpp, e);
			return null;
		}
	}

	private FetchResult buildFetchResult( //
	      final Tpp tpp, final List<MessagePriority> msgs, final String filter) {
		if (CollectionUtil.isNotEmpty(msgs)) {
			FetchResult result = new FetchResult();

			long biggestOffset = 0L;
			final TppConsumerMessageBatch batch = new TppConsumerMessageBatch();
			Iterator<MessagePriority> iter = msgs.iterator();
			while (iter.hasNext()) {
				MessagePriority dataObj = iter.next();
				biggestOffset = Math.max(biggestOffset, dataObj.getId());
				if (matchesFilter(tpp.getTopic(), dataObj, filter)) {
					MessageMeta msgMeta = new MessageMeta(dataObj.getId(), 0, dataObj.getId(), tpp.getPriorityInt(), false);
					batch.addMessageMeta(msgMeta);
				} else {
					iter.remove();
				}
			}

			if (biggestOffset > 0 && msgs.isEmpty()) {
				BrokerDummyMessagePriority msg = new BrokerDummyMessagePriority(tpp, biggestOffset);
				msgs.add(msg);
				batch.addMessageMeta(new DummyMessageMeta(msg.getId(), 0, msg.getId(), msg.getPriority(), false));
			}

			final String topic = tpp.getTopic();
			batch.setTopic(topic);
			batch.setPartition(tpp.getPartition());
			batch.setResend(false);
			batch.setPriority(tpp.getPriorityInt());

			batch.setTransferCallback(new TransferCallback() {
				@Override
				public void transfer(ByteBuf out) {
					for (MessagePriority dataObj : msgs) {
						try {
							PartialDecodedMessage partialMsg = new PartialDecodedMessage();
							partialMsg.setRemainingRetries(0);
							partialMsg.setDurableProperties(Unpooled.wrappedBuffer(dataObj.getAttributes()));
							if (dataObj.getPayload() instanceof ByteArrayInputStream) {
								partialMsg.setBody(Unpooled.wrappedBuffer(getBytes((ByteArrayInputStream) dataObj.getPayload())));
							} else {
								partialMsg.setBody(Unpooled.wrappedBuffer(IO.INSTANCE.readFrom(dataObj.getPayload())));
							}
							partialMsg.setBornTime(dataObj.getCreationDate().getTime());
							partialMsg.setKey(dataObj.getRefKey());
							partialMsg.setBodyCodecType(dataObj.getCodecType());

							m_messageCodec.encodePartial(partialMsg, out);
						} catch (Exception e) {
							log.error("Exception occurred in storage's TransferCallback.", e);
						}
					}
				}
			});

			result.setBatch(batch);
			result.setOffset(biggestOffset);
			return result;
		}

		return null;
	}

	private byte[] getBytes(ByteArrayInputStream bais) {
		try {
			return (byte[]) m_bufFieldOfByteArrayInputStream.get(bais);
		} catch (Exception e) {
			throw new RuntimeException("Failed to get bytes from ByteArrayInputStream", e);
		}
	}

	private Map<String, String> getAppProperties(ByteBuf buf) {
		Map<String, String> map = new HashMap<>();
		if (buf != null) {
			HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
			byte firstByte = codec.readByte();
			if (HermesPrimitiveCodec.NULL != firstByte) {
				codec.readerIndexBack(1);
				int length = codec.readInt();
				if (length > 0) {
					for (int i = 0; i < length; i++) {
						String key = codec.readSuffixStringWithPrefix(PropertiesHolder.APP, true);
						if (!StringUtils.isBlank(key)) {
							map.put(key, codec.readString());
						} else {
							codec.skipString();
						}
					}
				}
			}
		}
		return map;
	}

	private boolean matchesFilter(String topic, MessagePriority dataObj, String filterString) {
		if (StringUtils.isBlank(filterString)) {
			return true;
		}
		ByteBuf byteBuf = Unpooled.wrappedBuffer(dataObj.getAttributes());
		try {
			return m_filter.isMatch(topic, filterString, getAppProperties(byteBuf));
		} catch (Exception e) {
			log.error("Can not find filter for: {}" + filterString);
			return false;
		}
	}

	private FetchResult buildFetchResult(Tpg tpg, final List<ResendGroupId> msgs) {
		if (CollectionUtil.isNotEmpty(msgs)) {

			FetchResult result = new FetchResult();

			TppConsumerMessageBatch batch = new TppConsumerMessageBatch();
			ResendGroupId latestResend = new ResendGroupId();
			latestResend.setScheduleDate(new Date(0));
			latestResend.setId(0L);

			for (ResendGroupId dataObj : msgs) {
				if (resendAfter(dataObj, latestResend)) {
					latestResend = dataObj;
				}
				MessageMeta msgMeta = new MessageMeta(dataObj.getId(), dataObj.getRemainingRetries(),
				      dataObj.getOriginId(), dataObj.getPriority(), true);

				batch.addMessageMeta(msgMeta);

			}
			final String topic = tpg.getTopic();
			batch.setTopic(topic);
			batch.setPartition(tpg.getPartition());
			batch.setResend(true);

			batch.setTransferCallback(new TransferCallback() {

				@Override
				public void transfer(ByteBuf out) {
					for (ResendGroupId dataObj : msgs) {
						try {
							PartialDecodedMessage partialMsg = new PartialDecodedMessage();
							partialMsg.setRemainingRetries(dataObj.getRemainingRetries());
							partialMsg.setDurableProperties(Unpooled.wrappedBuffer(dataObj.getAttributes()));
							if (dataObj.getPayload() instanceof ByteArrayInputStream) {
								partialMsg.setBody(Unpooled.wrappedBuffer(getBytes((ByteArrayInputStream) dataObj.getPayload())));
							} else {
								partialMsg.setBody(Unpooled.wrappedBuffer(IO.INSTANCE.readFrom(dataObj.getPayload())));
							}
							partialMsg.setBornTime(dataObj.getCreationDate().getTime());
							partialMsg.setKey(dataObj.getRefKey());
							partialMsg.setBodyCodecType(dataObj.getCodecType());

							m_messageCodec.encodePartial(partialMsg, out);
						} catch (Exception e) {
							log.error("Exception occurred in storage's TransferCallback.", e);
						}
					}
				}

			});

			result.setBatch(batch);
			result.setOffset(new Pair<Date, Long>(latestResend.getScheduleDate(), latestResend.getId()));
			return result;
		}

		return null;
	}

	@Override
	public void nack(Tpp tpp, String groupId, boolean resend, List<Pair<Long, MessageMeta>> msgId2Metas) {
		if (CollectionUtil.isNotEmpty(msgId2Metas)) {
			try {

				RetryPolicy retryPolicy = m_metaService.findRetryPolicyByTopicAndGroup(tpp.getTopic(), groupId);

				List<Pair<Long, MessageMeta>> toDeadLetter = new ArrayList<>();
				List<Pair<Long, MessageMeta>> toResend = new ArrayList<>();
				for (Pair<Long, MessageMeta> pair : msgId2Metas) {
					MessageMeta meta = pair.getValue();
					if (resend) {
						meta.setRemainingRetries(meta.getRemainingRetries() - 1);
					} else {
						meta.setRemainingRetries(retryPolicy.getRetryTimes());
					}

					if (meta.getRemainingRetries() <= 0) {
						toDeadLetter.add(pair);
					} else {
						toResend.add(pair);
					}

				}

				copyToDeadLetter(tpp, groupId, toDeadLetter, resend);
				copyToResend(tpp, groupId, toResend, resend, retryPolicy);
			} catch (Exception e) {
				log.error("Failed to nack messages(topic={}, partition={}, priority={}, groupId={}).", tpp.getTopic(),
				      tpp.getPartition(), tpp.isPriority(), groupId, e);
			}
		}
	}

	private void copyToResend(Tpp tpp, String groupId, List<Pair<Long, MessageMeta>> msgId2Metas, boolean resend,
	      RetryPolicy retryPolicy) throws DalException {
		if (CollectionUtil.isNotEmpty(msgId2Metas)) {
			long now = m_systemClockService.now();

			if (!resend) {
				ResendGroupId proto = new ResendGroupId();
				proto.setTopic(tpp.getTopic());
				proto.setPartition(tpp.getPartition());
				proto.setPriority(tpp.getPriorityInt());
				proto.setGroupId(m_metaService.translateToIntGroupId(tpp.getTopic(), groupId));
				proto.setScheduleDate(new Date(retryPolicy.nextScheduleTimeMillis(0, now)));
				proto.setMessageIds(collectOffset(msgId2Metas));
				proto.setRemainingRetries(retryPolicy.getRetryTimes());
				proto.setCreationDate(proto.getCreationDate());

				m_resendDao.copyFromMessageTable(proto);
			} else {
				int intGroupId = m_metaService.translateToIntGroupId(tpp.getTopic(), groupId);
				Map<Long, Integer> id2RemainingRetries = new HashMap<Long, Integer>();
				for (Pair<Long, MessageMeta> pair : msgId2Metas) {
					id2RemainingRetries.put(pair.getKey(), pair.getValue().getRemainingRetries());
				}

				Long[] pks = id2RemainingRetries.keySet().toArray(new Long[id2RemainingRetries.size()]);
				List<ResendGroupId> resends = m_resendDao.findByPKs(tpp.getTopic(), tpp.getPartition(), intGroupId, pks,
				      ResendGroupIdEntity.READSET_FULL);

				for (ResendGroupId r : resends) {
					r.setTopic(tpp.getTopic());
					r.setPartition(tpp.getPartition());
					r.setGroupId(intGroupId);

					int retryTimes = retryPolicy.getRetryTimes() - id2RemainingRetries.get(r.getId());
					r.setScheduleDate(new Date(retryPolicy.nextScheduleTimeMillis(retryTimes, now)));
					r.setRemainingRetries(r.getRemainingRetries() - 1);
					r.setCreationDate(r.getCreationDate());
				}
				m_resendDao.insert(resends.toArray(new ResendGroupId[resends.size()]));
			}

		}
	}

	private void copyToDeadLetter(Tpp tpp, String groupId, List<Pair<Long, MessageMeta>> msgId2Metas, boolean resend)
	      throws DalException {
		if (CollectionUtil.isNotEmpty(msgId2Metas)) {
			DeadLetter proto = new DeadLetter();
			proto.setTopic(tpp.getTopic());
			proto.setPartition(tpp.getPartition());
			proto.setPriority(tpp.getPriorityInt());
			proto.setGroupId(m_metaService.translateToIntGroupId(tpp.getTopic(), groupId));
			proto.setDeadDate(new Date());
			proto.setMessageIds(collectOffset(msgId2Metas));

			if (resend) {
				m_deadLetterDao.copyFromResendTable(proto);
			} else {
				m_deadLetterDao.copyFromMessageTable(proto);
			}
		}
	}

	private Long[] collectOffset(List<Pair<Long, MessageMeta>> msgId2Metas) {
		Long[] offsets = new Long[msgId2Metas.size()];

		int idx = 0;
		for (Pair<Long, MessageMeta> pair : msgId2Metas) {
			offsets[idx++] = pair.getKey();
		}

		return offsets;
	}

	@Override
	public void ack(Tpp tpp, String groupId, boolean resend, long msgSeq) {
		try {
			String topic = tpp.getTopic();
			int partition = tpp.getPartition();
			int intGroupId = m_metaService.translateToIntGroupId(tpp.getTopic(), groupId);
			if (resend) {
				OffsetResend proto = getOffsetResend(topic, partition, intGroupId);

				proto.setTopic(topic);
				proto.setPartition(partition);
				proto.setLastScheduleDate(new Date());
				proto.setLastId(msgSeq);

				m_ackFlusher.ackOffsetResend(proto);
			} else {
				OffsetMessage proto = getOffsetMessage(tpp, intGroupId);
				proto.setTopic(topic);
				proto.setPartition(partition);
				proto.setOffset(msgSeq);

				m_ackFlusher.ackOffsetMessage(proto);
			}
		} catch (DalException e) {
			log.error("Failed to ack messages(topic={}, partition={}, priority={}, groupId={}).", tpp.getTopic(),
			      tpp.getPartition(), tpp.isPriority(), groupId, e);
		}
	}

	private OffsetMessage getOffsetMessage(Tpp tpp, int intGroupId) throws DalException {
		Pair<Tpp, Integer> key = new Pair<>(tpp, intGroupId);

		if (!m_offsetMessageCache.containsKey(key)) {
			synchronized (m_offsetMessageCache) {
				if (!m_offsetMessageCache.containsKey(key)) {
					m_offsetMessageCache.put(key,
					      findLastOffset(tpp.getTopic(), tpp.getPartition(), tpp.getPriorityInt(), intGroupId));
				}
			}
		}
		return m_offsetMessageCache.get(key);
	}

	private OffsetResend getOffsetResend(String topic, int partition, int intGroupId) throws DalException {
		Triple<String, Integer, Integer> tpg = new Triple<String, Integer, Integer>(topic, partition, intGroupId);

		if (!m_offsetResendCache.containsKey(tpg)) {
			synchronized (m_offsetResendCache) {
				if (!m_offsetResendCache.containsKey(tpg)) {
					m_offsetResendCache.put(tpg, findLastResendOffset(topic, partition, intGroupId));
				}
			}
		}
		return m_offsetResendCache.get(tpg);
	}

	@Override
	public synchronized Object findLastResendOffset(Tpg tpg) throws DalException {
		int intGroupId = m_metaService.translateToIntGroupId(tpg.getTopic(), tpg.getGroupId());
		OffsetResend offset = findLastResendOffset(tpg.getTopic(), tpg.getPartition(), intGroupId);
		return new Pair<>(offset.getLastScheduleDate(), offset.getLastId());
	}

	private synchronized OffsetResend findLastResendOffset(String topic, int partition, int intGroupId)
	      throws DalException {
		List<OffsetResend> tops = m_offsetResendDao.top(topic, partition, intGroupId, OffsetResendEntity.READSET_FULL);
		if (CollectionUtil.isNotEmpty(tops)) {
			OffsetResend top = CollectionUtil.first(tops);
			return top;
		} else {
			List<ResendGroupId> topMsg = m_resendDao.topId(topic, partition, intGroupId, ResendGroupIdEntity.READSET_ID);

			long lastId = 0L;
			if (!topMsg.isEmpty()) {
				lastId = CollectionUtil.last(topMsg).getId();
			}

			OffsetResend proto = new OffsetResend();
			proto.setTopic(topic);
			proto.setPartition(partition);
			proto.setGroupId(intGroupId);
			proto.setLastScheduleDate(new Date(0));
			proto.setLastId(lastId);
			proto.setCreationDate(new Date());

			m_offsetResendDao.insert(proto);
			return proto;
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public FetchResult fetchResendMessages(Tpg tpg, Object startOffset, int batchSize) {
		Pair<Date, Long> startPair = (Pair<Date, Long>) startOffset;

		try {
			int groupId = m_metaService.translateToIntGroupId(tpg.getTopic(), tpg.getGroupId());
			Long maxId = findMaxDuedResendId(tpg, groupId);

			if (maxId != null) {
				final List<ResendGroupId> dataObjs = m_resendDao.find(tpg.getTopic(), tpg.getPartition(), groupId,
				      batchSize, startPair.getValue(), maxId, ResendGroupIdEntity.READSET_FULL);

				return buildFetchResult(tpg, dataObjs);
			}
		} catch (DalException e) {
			log.error("Failed to fetch resend messages(topic={}, partition={}, groupId={}).", tpg.getTopic(),
			      tpg.getPartition(), tpg.getGroupId(), e);
		}

		return null;
	}

	private Long findMaxDuedResendId(Tpg tpg, int groupId) throws DalException {
		List<ResendGroupId> maxDuedRow = m_resendDao.findMaxDuedScheduleDate(tpg.getTopic(), tpg.getPartition(), groupId,
		      new Date(m_systemClockService.now()), ResendGroupIdEntity.READSET_SCHEDULE_DATE);
		if (CollectionUtil.isNotEmpty(maxDuedRow)) {
			Date maxDuedScheduleDate = maxDuedRow.get(0).getScheduleDate();
			List<ResendGroupId> maxDuedId = m_resendDao.findMaxIdByScheduleDate(tpg.getTopic(), tpg.getPartition(),
			      groupId, maxDuedScheduleDate, ResendGroupIdEntity.READSET_ID);
			if (CollectionUtil.isNotEmpty(maxDuedId)) {
				return maxDuedId.get(0).getId();
			}
		}

		return null;
	}

	private boolean resendAfter(ResendGroupId l, ResendGroupId r) {
		if (l.getScheduleDate().after(r.getScheduleDate())) {
			return true;
		}
		if (l.getScheduleDate().equals(r.getScheduleDate()) && l.getId() > r.getId()) {
			return true;
		}

		return false;
	}

	// return 0 if not found
	@Override
	public Object findMessageOffsetByTime(Tpp tpp, long time) {
		if (!hasStorageForPriority(tpp.getTopic(), tpp.getPriorityInt())) {
			return 0L;
		}

		MessagePriority oldestMsg = findOldestMessageOffset(tpp);
		MessagePriority latestMsg = findLatestMessageOffset(tpp);

		if (oldestMsg == null || latestMsg == null) {
			return 0L;
		}

		long offset = Long.MIN_VALUE == time ? oldestMsg.getId() //
		      : Long.MAX_VALUE == time ? latestMsg.getId() //
		            : findMessageOffsetByTimeInRange(tpp, oldestMsg, latestMsg, time);

		return offset <= 0 ? 0L : offset - 1L;
	}

	private MessagePriority findOldestMessageOffset(Tpp tpp) {
		try {
			return m_msgDao.findOldestOffset( //
			      tpp.getTopic(), tpp.getPartition(), tpp.getPriorityInt(), READSET_OFFSET);
		} catch (Exception e) {
			log.warn("Find oldest message offset failed.{}", tpp);
			return null;
		}
	}

	private MessagePriority findLatestMessageOffset(Tpp tpp) {
		try {
			List<MessagePriority> msgs = m_msgDao.findLatestOffset( //
			      tpp.getTopic(), tpp.getPartition(), tpp.getPriorityInt(), READSET_OFFSET);
			if (msgs != null && msgs.size() > 0) {
				return msgs.get(0);
			}
		} catch (Exception e) {
			log.warn("Find latest message offset failed.{}", tpp);
		}
		return null;
	}

	private long findMessageOffsetByTimeInRange(Tpp tpp, MessagePriority left, MessagePriority right, long time) {
		long precisionMillis = m_config.getMessageOffsetQueryPrecisionMillis();

		if (left.getId() == right.getId()) {
			return compareWithPrecision(left.getCreationDate().getTime(), time, precisionMillis) == 0 ? left.getId() : 0L;
		}

		if (compareWithPrecision(left.getCreationDate().getTime(), time, precisionMillis) >= 0) {
			return left.getId();
		}

		if (compareWithPrecision(right.getCreationDate().getTime(), time, precisionMillis) <= 0) {
			return right.getId();
		}

		try {
			long leftId = left.getId();
			long rightId = right.getId();
			while (leftId <= rightId) {
				long midId = leftId + (rightId - leftId) / 2L;

				MessagePriority smallestIdGEMidId = m_msgDao.findOffsetGreaterOrEqual(tpp.getTopic(), tpp.getPartition(),
				      tpp.getPriorityInt(), midId, READSET_OFFSET);

				MessagePriority largestIdLEMidId = m_msgDao.findOffsetLessOrEqual(tpp.getTopic(), tpp.getPartition(),
				      tpp.getPriorityInt(), midId, READSET_OFFSET);

				if (compareWithPrecision(smallestIdGEMidId.getCreationDate().getTime(), time, precisionMillis) < 0) {
					leftId = smallestIdGEMidId.getId() + 1;
				} else if (compareWithPrecision(largestIdLEMidId.getCreationDate().getTime(), time, precisionMillis) > 0) {
					rightId = largestIdLEMidId.getId() - 1;
				} else if (compareWithPrecision(largestIdLEMidId.getCreationDate().getTime(), time, precisionMillis) == 0) {
					return largestIdLEMidId.getId();
				} else if (compareWithPrecision(smallestIdGEMidId.getCreationDate().getTime(), time, precisionMillis) == 0) {
					return smallestIdGEMidId.getId();
				} else {
					return largestIdLEMidId.getId();
				}
			}

			MessagePriority msg = m_msgDao.findOffsetGreaterOrEqual(tpp.getTopic(), tpp.getPartition(),
			      tpp.getPriorityInt(), rightId, READSET_OFFSET);
			return msg.getId();
		} catch (Exception e) {
			if (log.isDebugEnabled()) {
				log.debug("Find message by offset failed. {}", tpp, e);
			}
		}
		return 0L;
	}

	private int compareWithPrecision(long src, long dst, long precisionMillis) {
		long diff = src - dst;
		if (Math.abs(diff) <= precisionMillis) {
			return 0;
		} else {
			return diff > 0L ? 1 : -1;
		}
	}

	private boolean hasStorageForPriority(String topicName, int priority) {
		Topic topic = m_metaService.findTopicByName(topicName);
		if (topic != null) {
			if (topic.isPriorityMessageEnabled()) {
				return true;
			} else {
				return priority != 0;
			}
		} else {
			return false;
		}
	}

	@Override
	public void initialize() throws InitializationException {

		try {
			m_bufFieldOfByteArrayInputStream = ByteArrayInputStream.class.getDeclaredField("buf");
			m_bufFieldOfByteArrayInputStream.setAccessible(true);
		} catch (Exception e) {
			throw new InitializationException("Failed to get field \"buf\" from ByteArrayInputStream.", e);
		}

		if (m_config.getMySQLCacheConfig().isEnabled()) {
			m_msgDao = CachedMessagePriorityDaoInterceptor.createProxy(m_msgDao, m_config.getMySQLCacheConfig());
		}

		m_catSelectorByPriorityMetrics.put(1, CatConstants.TYPE_MESSAGE_PRODUCE_BY_PRIORITY + "1");
		m_catSelectorByPriorityMetrics.put(10, CatConstants.TYPE_MESSAGE_PRODUCE_BY_PRIORITY + "2-10");
		m_catSelectorByPriorityMetrics.put(50, CatConstants.TYPE_MESSAGE_PRODUCE_BY_PRIORITY + "11-50");
		m_catSelectorByPriorityMetrics.put(Integer.MAX_VALUE, CatConstants.TYPE_MESSAGE_PRODUCE_BY_PRIORITY + "gt-50");

		m_catSelectorByNonPriorityMetrics.put(1, CatConstants.TYPE_MESSAGE_PRODUCE_BY_NONPRIORITY + "1");
		m_catSelectorByNonPriorityMetrics.put(10, CatConstants.TYPE_MESSAGE_PRODUCE_BY_NONPRIORITY + "2-10");
		m_catSelectorByNonPriorityMetrics.put(50, CatConstants.TYPE_MESSAGE_PRODUCE_BY_NONPRIORITY + "11-50");
		m_catSelectorByNonPriorityMetrics.put(Integer.MAX_VALUE, CatConstants.TYPE_MESSAGE_PRODUCE_BY_NONPRIORITY
		      + "gt-50");
	}
}
