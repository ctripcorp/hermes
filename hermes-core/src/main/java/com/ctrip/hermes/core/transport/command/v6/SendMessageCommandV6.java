package com.ctrip.hermes.core.transport.command.v6;

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
import java.util.concurrent.atomic.AtomicLong;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.bo.SendMessageResult;
import com.ctrip.hermes.core.exception.MessageSendException;
import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.message.codec.MessageCodec;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.core.transport.ManualRelease;
import com.ctrip.hermes.core.transport.command.AbstractCommand;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.MessageBatchWithRawData;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@ManualRelease
public class SendMessageCommandV6 extends AbstractCommand {

	private static final long serialVersionUID = 8443575812437722822L;

	private AtomicInteger m_msgCounter = new AtomicInteger(0);

	private String m_topic;

	private int m_partition;

	private long m_timeout;

	private ConcurrentMap<Integer, List<ProducerMessage<?>>> m_msgs = new ConcurrentHashMap<>();

	private transient Map<Integer, MessageBatchWithRawData> m_decodedBatches = new HashMap<>();

	private transient Map<Integer, Pair<SettableFuture<SendResult>, ProducerMessage<?>>> m_futures = new HashMap<>();

	private transient long m_bornTime = System.currentTimeMillis();
	
	private transient AtomicLong m_selectorOffset = new AtomicLong();

	public SendMessageCommandV6() {
		this(null, -1, -1L);
	}

	public SendMessageCommandV6(String topic, int partition, long timeout) {
		super(CommandType.MESSAGE_SEND_V6, 6);
		m_topic = topic;
		m_partition = partition;
		m_timeout = timeout;
	}
	
	public long getSelectorOffset() {
		return m_selectorOffset.get();
	}

	public void setSelectorOffset(long selectorOffset) {
		m_selectorOffset.set(selectorOffset);
	}

	public Collection<Pair<SettableFuture<SendResult>, ProducerMessage<?>>> getFutureAndMessagePair() {
		return m_futures.values();
	}

	public long getBornTime() {
		return m_bornTime;
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

		m_futures.put(msgSeqNo, new Pair<SettableFuture<SendResult>, ProducerMessage<?>>(future, msg));
	}

	private void validate(ProducerMessage<?> msg) {
		if (!m_topic.equals(msg.getTopic()) || m_partition != msg.getPartition()) {
			throw new IllegalArgumentException(String.format(
			      "Illegal message[topic=%s, partition=%s] try to add to SendMessageCommandV5[topic=%s, partition=%s]",
			      msg.getTopic(), msg.getPartition(), m_topic, m_partition));
		}
	}

	public Map<Integer, MessageBatchWithRawData> getMessageRawDataBatches() {
		return m_decodedBatches;
	}

	public int getMessageCount() {
		return m_msgCounter.get();
	}

	public void onResultReceived(SendMessageResultCommandV6 result) {
		for (Map.Entry<Integer, Pair<SettableFuture<SendResult>, ProducerMessage<?>>> entry : m_futures.entrySet()) {
			SendMessageResult sendResult = result.getSendResult(entry.getKey());
			if (sendResult.isSuccess()) {
				entry.getValue().getKey().set(new SendResult(entry.getValue().getValue()));
			} else {
				MessageSendException sendException = new MessageSendException(
				      sendResult.getErrorMessage() == null ? "Send failed" : sendResult.getErrorMessage(), entry.getValue()
				            .getValue());
				entry.getValue().getKey().setException(sendException);
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
		for (Map.Entry<Integer, List<ProducerMessage<?>>> entry : msgs.entrySet()) {
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

		return new MessageBatchWithRawData(topic, msgSeqs, rawData, getTargetIdc());

	}

	public Collection<List<ProducerMessage<?>>> getProducerMessages() {
		return m_msgs.values();
	}

}
