package com.ctrip.hermes.core.transport.command.v3;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.exception.MessageSendException;
import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.message.codec.MessageCodec;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.core.transport.ManualRelease;
import com.ctrip.hermes.core.transport.command.AbstractCommand;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.MessageBatchWithRawData;
import com.ctrip.hermes.core.transport.command.SendMessageResultCommand;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@ManualRelease
public class SendMessageCommandV3 extends AbstractCommand {

	private static final long serialVersionUID = 8443575812437722822L;

	private AtomicInteger m_msgCounter = new AtomicInteger(0);

	private String m_topic;

	private int m_partition;

	private long m_timeout;

	private ConcurrentMap<Integer, List<ProducerMessage<?>>> m_msgs = new ConcurrentHashMap<Integer, List<ProducerMessage<?>>>();

	private transient Map<Integer, MessageBatchWithRawData> m_decodedBatches = new HashMap<Integer, MessageBatchWithRawData>();

	private transient Map<Integer, SettableFuture<SendResult>> m_futures = new HashMap<Integer, SettableFuture<SendResult>>();

	public SendMessageCommandV3() {
		this(null, -1, -1L);
	}

	public SendMessageCommandV3(String topic, int partition, long timeout) {
		super(CommandType.MESSAGE_SEND_V3, 3);
		m_topic = topic;
		m_partition = partition;
		m_timeout = timeout;
	}

	public ConcurrentMap<Integer, List<ProducerMessage<?>>> getMsgs() {
		return m_msgs;
	}

	public String getTopic() {
		return m_topic;
	}

	public int getPartition() {
		return m_partition;
	}

	public long getTimeout() {
		return m_timeout;
	}

	public void addMessage(ProducerMessage<?> msg, SettableFuture<SendResult> future) {
		validate(msg);

		int msgSeqNo = m_msgCounter.getAndIncrement();
		msg.setMsgSeqNo(msgSeqNo);

		if (msg.isPriority()) {
			if (!m_msgs.containsKey(0)) {
				m_msgs.putIfAbsent(0, new LinkedList<ProducerMessage<?>>());
			}
			m_msgs.get(0).add(msg);
		} else {
			if (!m_msgs.containsKey(1)) {
				m_msgs.putIfAbsent(1, new LinkedList<ProducerMessage<?>>());
			}
			m_msgs.get(1).add(msg);
		}

		m_futures.put(msgSeqNo, future);
	}

	private void validate(ProducerMessage<?> msg) {
		if (!m_topic.equals(msg.getTopic()) || m_partition != msg.getPartition()) {
			throw new IllegalArgumentException(String.format(
			      "Illegal message[topic=%s, partition=%s] try to add to SendMessageCommandV3[topic=%s, partition=%s]",
			      msg.getTopic(), msg.getPartition(), m_topic, m_partition));
		}
	}

	public Map<Integer, MessageBatchWithRawData> getMessageRawDataBatches() {
		return m_decodedBatches;
	}

	public int getMessageCount() {
		return m_msgCounter.get();
	}

	public void onResultReceived(SendMessageResultCommand result) {
		for (Map.Entry<Integer, SettableFuture<SendResult>> entry : m_futures.entrySet()) {
			// FIXME add more details into result or exception, no matter success or not(offset, id, etc.)
			if (result.isSuccess(entry.getKey())) {
				entry.getValue().set(new SendResult());
			} else {
				entry.getValue().setException(new MessageSendException("Send failed", null));
			}
		}
	}

	@Override
	public void parse0(ByteBuf buf) {
		m_rawBuf = buf;

		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);

		m_msgCounter.set(codec.readInt());

		m_topic = codec.readString();
		m_partition = codec.readInt();
		m_timeout = codec.readLong();

		readDatas(buf, codec, m_topic);

	}

	@Override
	public void toBytes0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);

		codec.writeInt(m_msgCounter.get());

		codec.writeString(m_topic);
		codec.writeInt(m_partition);
		codec.writeLong(m_timeout);

		writeDatas(buf, codec, m_msgs);
	}

	private void writeDatas(ByteBuf buf, HermesPrimitiveCodec codec, Map<Integer, List<ProducerMessage<?>>> msgs) {
		codec.writeInt(msgs.size());
		for (Map.Entry<Integer, List<ProducerMessage<?>>> entry : m_msgs.entrySet()) {
			// priority flag
			codec.writeInt(entry.getKey());

			writeMsgs(entry.getValue(), codec, buf);
		}
	}

	private void writeMsgs(List<ProducerMessage<?>> msgs, HermesPrimitiveCodec codec, ByteBuf buf) {
		MessageCodec msgCodec = PlexusComponentLocator.lookup(MessageCodec.class);
		// write msgSeqs
		codec.writeInt(msgs.size());

		// seqNos
		for (ProducerMessage<?> msg : msgs) {
			codec.writeInt(msg.getMsgSeqNo());
		}

		// placeholder for payload len
		int indexBeforeLen = buf.writerIndex();
		codec.writeInt(-1);

		int indexBeforePayload = buf.writerIndex();
		// payload
		for (ProducerMessage<?> msg : msgs) {
			msgCodec.encode(msg, buf);
		}
		int indexAfterPayload = buf.writerIndex();
		int payloadLen = indexAfterPayload - indexBeforePayload;

		// refill payload len
		buf.writerIndex(indexBeforeLen);
		codec.writeInt(payloadLen);

		buf.writerIndex(indexAfterPayload);
	}

	private void readDatas(ByteBuf buf, HermesPrimitiveCodec codec, String topic) {
		int size = codec.readInt();
		for (int i = 0; i < size; i++) {
			int priority = codec.readInt();

			m_decodedBatches.put(priority, readMsgs(topic, codec, buf));
		}

	}

	private MessageBatchWithRawData readMsgs(String topic, HermesPrimitiveCodec codec, ByteBuf buf) {
		int size = codec.readInt();

		List<Integer> msgSeqs = new ArrayList<Integer>();

		for (int j = 0; j < size; j++) {
			msgSeqs.add(codec.readInt());
		}

		int payloadLen = codec.readInt();

		ByteBuf rawData = buf.readSlice(payloadLen);

		return new MessageBatchWithRawData(topic, msgSeqs, rawData, null);

	}

	public Collection<List<ProducerMessage<?>>> getProducerMessages() {
		return m_msgs.values();
	}

	public List<Pair<ProducerMessage<?>, SettableFuture<SendResult>>> getProducerMessageFuturePairs() {
		List<Pair<ProducerMessage<?>, SettableFuture<SendResult>>> pairs = new LinkedList<Pair<ProducerMessage<?>, SettableFuture<SendResult>>>();
		Collection<List<ProducerMessage<?>>> msgsList = getProducerMessages();
		for (List<ProducerMessage<?>> msgs : msgsList) {
			for (ProducerMessage<?> msg : msgs) {
				SettableFuture<SendResult> future = m_futures.get(msg.getMsgSeqNo());
				if (future != null) {
					pairs.add(new Pair<ProducerMessage<?>, SettableFuture<SendResult>>(msg, future));
				}
			}
		}

		return pairs;
	}

	public Map<Integer, SettableFuture<SendResult>> getFutures() {
		return m_futures;
	}

}
