package com.ctrip.hermes.core.transport.command;

import io.netty.buffer.ByteBuf;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class SendMessageResultCommand extends AbstractCommand {

	private static final long serialVersionUID = -2408812182538982540L;

	private Map<Integer, Boolean> m_successes = new ConcurrentHashMap<Integer, Boolean>();

	private int m_totalSize;

	public SendMessageResultCommand() {
		this(0);
	}

	public SendMessageResultCommand(int totalSize) {
		super(CommandType.RESULT_MESSAGE_SEND, 1);
		m_totalSize = totalSize;
	}

	public boolean isAllResultsSet() {
		return m_successes.size() == m_totalSize;
	}

	public void addResults(Map<Integer, Boolean> results) {
		m_successes.putAll(results);
	}

	public boolean isSuccess(Integer msgSeqNo) {
		if (m_successes.containsKey(msgSeqNo)) {
			return m_successes.get(msgSeqNo);
		}
		return false;
	}

	@Override
	public void parse0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		m_totalSize = codec.readInt();
		m_successes = codec.readIntBooleanMap();
	}

	@Override
	public void toBytes0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		codec.writeInt(m_totalSize);
		codec.writeIntBooleanMap(m_successes);
	}

	public Map<Integer, Boolean> getSuccesses() {
		return m_successes;
	}

}
