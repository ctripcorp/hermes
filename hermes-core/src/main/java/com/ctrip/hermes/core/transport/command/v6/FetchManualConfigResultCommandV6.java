package com.ctrip.hermes.core.transport.command.v6;

import io.netty.buffer.ByteBuf;

import com.ctrip.hermes.core.transport.command.AbstractCommand;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class FetchManualConfigResultCommandV6 extends AbstractCommand {

	private static final long serialVersionUID = 5507922065276703281L;

	private byte[] m_data;

	public FetchManualConfigResultCommandV6() {
		super(CommandType.RESULT_FETCH_MANUAL_CONFIG_V6, 6);
	}

	public byte[] getData() {
		return m_data;
	}

	public void setData(byte[] data) {
		m_data = data;
	}

	@Override
	public void parse0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		m_data = codec.readBytes();
	}

	@Override
	public void toBytes0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		codec.writeBytes(m_data);
	}

}
