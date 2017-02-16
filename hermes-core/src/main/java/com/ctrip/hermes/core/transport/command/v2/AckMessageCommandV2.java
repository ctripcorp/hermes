package com.ctrip.hermes.core.transport.command.v2;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.unidal.tuple.Triple;

import com.ctrip.hermes.core.bo.AckContext;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.transport.command.AbstractCommand;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class AckMessageCommandV2 extends AbstractCommand {

	private static final long serialVersionUID = 4241586688783048902L;

	public static final int NORMAL = 1;

	public static final int FORWARD_ONLY = 2;

	// key: tpp, groupId, isResend
	private ConcurrentMap<Triple<Tpp, String, Boolean>, List<AckContext>> m_ackMsgSeqs = new ConcurrentHashMap<Triple<Tpp, String, Boolean>, List<AckContext>>();

	// key: tpp, groupId, isResend
	private ConcurrentMap<Triple<Tpp, String, Boolean>, List<AckContext>> m_nackMsgSeqs = new ConcurrentHashMap<Triple<Tpp, String, Boolean>, List<AckContext>>();

	private int m_type = NORMAL;

	public AckMessageCommandV2() {
		this(NORMAL);
	}

	public AckMessageCommandV2(int type) {
		super(CommandType.MESSAGE_ACK_V2, 2);
		m_type = type;
	}

	@Override
	protected void toBytes0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		writeMsgSeqMap(codec, m_ackMsgSeqs);
		writeMsgSeqMap(codec, m_nackMsgSeqs);
		codec.writeInt(m_type);
	}

	@Override
	protected void parse0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		m_ackMsgSeqs = readMsgSeqMap(codec);
		m_nackMsgSeqs = readMsgSeqMap(codec);
		m_type = codec.readInt();
	}

	private void writeMsgSeqMap(HermesPrimitiveCodec codec,
	      ConcurrentMap<Triple<Tpp, String, Boolean>, List<AckContext>> msgSeqMap) {
		if (msgSeqMap == null) {
			codec.writeInt(0);
		} else {
			codec.writeInt(msgSeqMap.size());
			for (Triple<Tpp, String, Boolean> tppgr : msgSeqMap.keySet()) {
				Tpp tpp = tppgr.getFirst();
				codec.writeString(tpp.getTopic());
				codec.writeInt(tpp.getPartition());
				codec.writeInt(tpp.isPriority() ? 0 : 1);
				codec.writeString(tppgr.getMiddle());
				codec.writeBoolean(tppgr.getLast());
			}
			for (Triple<Tpp, String, Boolean> tppgr : msgSeqMap.keySet()) {
				List<AckContext> contexts = msgSeqMap.get(tppgr);

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

	private ConcurrentMap<Triple<Tpp, String, Boolean>, List<AckContext>> readMsgSeqMap(HermesPrimitiveCodec codec) {
		ConcurrentMap<Triple<Tpp, String, Boolean>, List<AckContext>> msgSeqMap = new ConcurrentHashMap<Triple<Tpp, String, Boolean>, List<AckContext>>();

		int mapSize = codec.readInt();
		if (mapSize != 0) {
			List<Triple<Tpp, String, Boolean>> tppgrs = new ArrayList<Triple<Tpp, String, Boolean>>();
			for (int i = 0; i < mapSize; i++) {
				Tpp tpp = new Tpp(codec.readString(), codec.readInt(), codec.readInt() == 0 ? true : false);
				String groupId = codec.readString();
				boolean resend = codec.readBoolean();
				tppgrs.add(new Triple<Tpp, String, Boolean>(tpp, groupId, resend));
			}
			for (int i = 0; i < mapSize; i++) {
				Triple<Tpp, String, Boolean> tppgr = tppgrs.get(i);

				int len = codec.readInt();
				if (len == 0) {
					msgSeqMap.put(tppgr, new ArrayList<AckContext>());
				} else {
					msgSeqMap.put(tppgr, new ArrayList<AckContext>(len));
				}

				for (int j = 0; j < len; j++) {
					msgSeqMap.get(tppgr).add(
					      new AckContext(codec.readLong(), codec.readInt(), codec.readLong(), codec.readLong()));
				}
			}
		}

		return msgSeqMap;
	}

	public void addAckMsg(Tpp tpp, String groupId, boolean resend, long msgSeq, int remainingRetries,
	      long onMessageStartTimeMillis, long onMessageEndTimeMillis) {
		Triple<Tpp, String, Boolean> key = new Triple<Tpp, String, Boolean>(tpp, groupId, resend);
		if (!m_ackMsgSeqs.containsKey(key)) {
			m_ackMsgSeqs.putIfAbsent(key, new ArrayList<AckContext>());
		}
		m_ackMsgSeqs.get(key).add(
		      new AckContext(msgSeq, remainingRetries, onMessageStartTimeMillis, onMessageEndTimeMillis));
	}

	public void addNackMsg(Tpp tpp, String groupId, boolean resend, long msgSeq, int remainingRetries,
	      long onMessageStartTimeMillis, long onMessageEndTimeMillis) {
		Triple<Tpp, String, Boolean> key = new Triple<Tpp, String, Boolean>(tpp, groupId, resend);
		if (!m_nackMsgSeqs.containsKey(key)) {
			m_nackMsgSeqs.putIfAbsent(key, new ArrayList<AckContext>());
		}
		m_nackMsgSeqs.get(key).add(
		      new AckContext(msgSeq, remainingRetries, onMessageStartTimeMillis, onMessageEndTimeMillis));
	}

	public ConcurrentMap<Triple<Tpp, String, Boolean>, List<AckContext>> getAckMsgs() {
		return m_ackMsgSeqs;
	}

	public ConcurrentMap<Triple<Tpp, String, Boolean>, List<AckContext>> getNackMsgs() {
		return m_nackMsgSeqs;
	}

	public int getType() {
		return m_type;
	}

}
