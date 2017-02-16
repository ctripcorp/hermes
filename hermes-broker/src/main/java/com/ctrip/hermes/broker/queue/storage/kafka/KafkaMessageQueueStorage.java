package com.ctrip.hermes.broker.queue.storage.kafka;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.broker.queue.storage.FetchResult;
import com.ctrip.hermes.broker.queue.storage.MessageQueueStorage;
import com.ctrip.hermes.broker.status.BrokerStatusMonitor;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.constants.CatConstants;
import com.ctrip.hermes.core.message.PartialDecodedMessage;
import com.ctrip.hermes.core.message.PropertiesHolder;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch.MessageMeta;
import com.ctrip.hermes.core.message.codec.MessageCodec;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.transport.command.MessageBatchWithRawData;
import com.ctrip.hermes.core.utils.CatUtil;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;
import com.ctrip.hermes.meta.entity.Storage;

@Named(type = MessageQueueStorage.class, value = Storage.KAFKA)
public class KafkaMessageQueueStorage implements MessageQueueStorage {

	private static final Logger log = LoggerFactory.getLogger(KafkaMessageQueueStorage.class);

	@Inject
	private MetaService m_metaService;

	@Inject
	private MessageCodec m_messageCodec;

	// TODO housekeeping
	private ConcurrentMap<Pair<String, String>, KafkaMessageBrokerSender> m_senders = new ConcurrentHashMap<>();

	@Override
	public void appendMessages(String topic, int partition, boolean priority, Collection<MessageBatchWithRawData> batches)
	      throws Exception {
		if (batches.isEmpty()) {
			return;
		}

		long totalBytes = 0;
		ByteBuf bodyBuf = Unpooled.buffer();
		try {
			for (MessageBatchWithRawData batch : batches) {
				KafkaMessageBrokerSender sender = getSender(topic, batch.getTargetIdc());
				List<PartialDecodedMessage> pdmsgs = batch.getMessages();
				for (PartialDecodedMessage pdmsg : pdmsgs) {
					m_messageCodec.encodePartial(pdmsg, bodyBuf);
					byte[] bytes = new byte[bodyBuf.readableBytes()];
					totalBytes += bytes.length;
					bodyBuf.readBytes(bytes);
					bodyBuf.clear();

					ByteBuf propertiesBuf = pdmsg.getDurableProperties();
					HermesPrimitiveCodec codec = new HermesPrimitiveCodec(propertiesBuf);
					Map<String, String> propertiesMap = codec.readStringStringMap();
					sender.send(topic, propertiesMap.get(PropertiesHolder.SYS + "pK"), bytes);
					BrokerStatusMonitor.INSTANCE.kafkaSend(topic);
				}
			}

			if (totalBytes > 0) {
				try {
					CatUtil
					      .logEventPeriodically(CatConstants.TYPE_MESSAGE_BROKER_PRODUCE_BYTES + "kafka", topic, totalBytes);
				} catch (Exception e) {
					log.warn("Exception occurred while loging bytes for {}-{}", topic, partition, e);
				}
			}

		} finally {
			bodyBuf.release();

		}
	}

	@Override
	public Object findLastOffset(Tpp tpp, int groupId) throws Exception {
		return null;
	}

	@Override
	public Object findLastResendOffset(Tpg tpg) throws Exception {
		return null;
	}

	@Override
	public FetchResult fetchMessages(Tpp tpp, Object startOffset, int batchSize, String filter) {
		return null;
	}

	@Override
	public FetchResult fetchResendMessages(Tpg tpg, Object startOffset, int batchSize) {
		return null;
	}

	@Override
	public void nack(Tpp tpp, String groupId, boolean resend, List<Pair<Long, MessageMeta>> msgId2Metas) {

	}

	@Override
	public void ack(Tpp tpp, String groupId, boolean resend, long msgSeq) {

	}

	private KafkaMessageBrokerSender getSender(String topic, String targetIdc) {
		Pair<String, String> topicIdcPair = new Pair<String, String>(topic, targetIdc);
		KafkaMessageBrokerSender sender = m_senders.get(topicIdcPair);

		if (sender == null) {
			m_senders.putIfAbsent(topicIdcPair, new KafkaMessageBrokerSender(topic, m_metaService, targetIdc));
			sender = m_senders.get(topicIdcPair);
		}

		return sender;
	}

	@Override
	public Object findMessageOffsetByTime(Tpp tpp, long time) {
		throw new UnsupportedOperationException("Kafka topic doesn't support this operation!");
	}

	@Override
	public FetchResult fetchMessages(Tpp tpp, List<Object> offsets) {
		throw new UnsupportedOperationException("Kafka topic doesn't support this operation!");
	}

}
