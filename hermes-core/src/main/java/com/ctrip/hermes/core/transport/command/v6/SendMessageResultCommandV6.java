package com.ctrip.hermes.core.transport.command.v6;

import io.netty.buffer.ByteBuf;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.ctrip.hermes.core.bo.SendMessageResult;
import com.ctrip.hermes.core.transport.command.AbstractCommand;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class SendMessageResultCommandV6 extends AbstractCommand {

	private static final long serialVersionUID = -2408812182538982540L;

	private Map<Integer, SendMessageResult> m_results = new ConcurrentHashMap<Integer, SendMessageResult>();

	private int m_totalSize;

	public SendMessageResultCommandV6() {
		this(0);
	}

	public SendMessageResultCommandV6(int totalSize) {
		super(CommandType.RESULT_MESSAGE_SEND_V6, 6);
		m_totalSize = totalSize;
	}

	public boolean isAllResultsSet() {
		return m_results.size() == m_totalSize;
	}

	public void addResults(Map<Integer, SendMessageResult> results) {
		m_results.putAll(results);
	}

	public SendMessageResult getSendResult(int msgSeqNo) {
		return m_results.get(msgSeqNo);
	}

	@Override
	public void parse0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		m_totalSize = codec.readInt();
		m_results = codec.readIntSendMessageResultMap();
	}

	@Override
	public void toBytes0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		codec.writeInt(m_totalSize);
		codec.writeIntSendMessageResultMap(m_results);
	}

	public Map<Integer, SendMessageResult> getResults() {
		return m_results;
	}

}
