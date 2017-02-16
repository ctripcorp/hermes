package com.ctrip.hermes.core.transport.command.v4;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ctrip.hermes.core.bo.AckContext;
import com.ctrip.hermes.core.transport.command.AbstractCommand;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class AckMessageCommandV4 extends AbstractCommand {

	private static final long serialVersionUID = -8004792374310009290L;

	private String m_topic;

	private int m_partition;

	private String m_group;

	private long m_timeout;

	private Map<Integer, List<AckContext>> m_ackedMsgs = new HashMap<>();

	private Map<Integer, List<AckContext>> m_ackedResendMsgs = new HashMap<>();

	private Map<Integer, List<AckContext>> m_nackedMsgs = new HashMap<>();

	private Map<Integer, List<AckContext>> m_nackedResendMsgs = new HashMap<>();

	public AckMessageCommandV4() {
		super(CommandType.MESSAGE_ACK_V4, 4);
	}

	public AckMessageCommandV4(String topic, int partition, String group, long timeout) {
		super(CommandType.MESSAGE_ACK_V4, 4);
		m_topic = topic;
		m_partition = partition;
		m_group = group;
		m_timeout = timeout;
	}

	public long getTimeout() {
		return m_timeout;
	}

	public String getTopic() {
		return m_topic;
	}

	public int getPartition() {
		return m_partition;
	}

	public String getGroup() {
		return m_group;
	}

	@Override
	protected void toBytes0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);

		codec.writeString(m_topic);
		codec.writeInt(m_partition);
		codec.writeString(m_group);
		codec.writeLong(m_timeout);

		writeMsgs(codec, m_ackedMsgs);
		writeMsgs(codec, m_ackedResendMsgs);
		writeMsgs(codec, m_nackedMsgs);
		writeMsgs(codec, m_nackedResendMsgs);
	}

	@Override
	protected void parse0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);

		m_topic = codec.readString();
		m_partition = codec.readInt();
		m_group = codec.readString();
		m_timeout = codec.readLong();

		m_ackedMsgs = readMsgs(codec);
		m_ackedResendMsgs = readMsgs(codec);
		m_nackedMsgs = readMsgs(codec);
		m_nackedResendMsgs = readMsgs(codec);
	}

	private void writeMsgs(HermesPrimitiveCodec codec, Map<Integer, List<AckContext>> msgs) {
		if (msgs == null) {
			codec.writeInt(0);
		} else {
			codec.writeInt(msgs.size());

			for (Integer priority : msgs.keySet()) {
				codec.writeInt(priority);
			}
			for (Integer priority : msgs.keySet()) {
				List<AckContext> contexts = msgs.get(priority);

				if (contexts == null || contexts.isEmpty()) {
					codec.writeInt(0);
				} else {
					codec.writeInt(contexts.size());
					for (AckContext context : contexts) {
						codec.writeLong(context.getMsgSeq());
						codec.writeInt(context.getRemainingRetries());
						codec.writeLong(context.getOnMessageStartTimeMillis());
						codec.writeLong(context.getOnMessageEndTimeMillis());
					}
				}
			}
		}
	}

	private Map<Integer, List<AckContext>> readMsgs(HermesPrimitiveCodec codec) {
		Map<Integer, List<AckContext>> msgSeqMap = new HashMap<>();

		int mapSize = codec.readInt();
		if (mapSize != 0) {
			List<Integer> priorities = new ArrayList<>();
			for (int i = 0; i < mapSize; i++) {
				priorities.add(codec.readInt());
			}

			for (int i = 0; i < mapSize; i++) {
				Integer priority = priorities.get(i);

				int len = codec.readInt();
				if (len == 0) {
					msgSeqMap.put(priority, new ArrayList<AckContext>());
				} else {
					msgSeqMap.put(priority, new ArrayList<AckContext>(len));
				}

				for (int j = 0; j < len; j++) {
					msgSeqMap.get(priority).add(
					      new AckContext(codec.readLong(), codec.readInt(), codec.readLong(), codec.readLong()));
				}
			}
		}

		return msgSeqMap;
	}

	public void addAckMsg(boolean isPriority, boolean isResend, long msgSeq, int remainingRetries,
	      long onMessageStartTimeMillis, long onMessageEndTimeMillis) {

		int priority = isPriority ? 0 : 1;

		Map<Integer, List<AckContext>> msgs = null;

		if (isResend) {
			msgs = m_ackedResendMsgs;
		} else {
			msgs = m_ackedMsgs;
		}

		if (!msgs.containsKey(priority)) {
			msgs.put(priority, new ArrayList<AckContext>());
		}
		msgs.get(priority)
		      .add(new AckContext(msgSeq, remainingRetries, onMessageStartTimeMillis, onMessageEndTimeMillis));

	}

	public void addNackMsg(boolean isPriority, boolean isResend, long msgSeq, int remainingRetries,
	      long onMessageStartTimeMillis, long onMessageEndTimeMillis) {

		int priority = isPriority ? 0 : 1;

		Map<Integer, List<AckContext>> msgs = null;

		if (isResend) {
			msgs = m_nackedResendMsgs;
		} else {
			msgs = m_nackedMsgs;
		}

		if (!msgs.containsKey(priority)) {
			msgs.put(priority, new ArrayList<AckContext>());
		}
		msgs.get(priority)
		      .add(new AckContext(msgSeq, remainingRetries, onMessageStartTimeMillis, onMessageEndTimeMillis));

	}

	public Map<Integer, List<AckContext>> getAckedMsgs() {
		return m_ackedMsgs;
	}

	public Map<Integer, List<AckContext>> getAckedResendMsgs() {
		return m_ackedResendMsgs;
	}

	public Map<Integer, List<AckContext>> getNackedMsgs() {
		return m_nackedMsgs;
	}

	public Map<Integer, List<AckContext>> getNackedResendMsgs() {
		return m_nackedResendMsgs;
	}

}
