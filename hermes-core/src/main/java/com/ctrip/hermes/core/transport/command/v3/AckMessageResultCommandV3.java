package com.ctrip.hermes.core.transport.command.v3;

import io.netty.buffer.ByteBuf;

import com.ctrip.hermes.core.transport.command.AbstractCommand;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class AckMessageResultCommandV3 extends AbstractCommand {

	private static final long serialVersionUID = -2462726426306841225L;

	private boolean m_success = false;

	public AckMessageResultCommandV3() {
		super(CommandType.RESULT_ACK_MESSAGE_V3, 3);
	}

	public void setSuccess(boolean success) {
		m_success = success;
	}

	public boolean isSuccess() {
		return m_success;
	}

	@Override
	public void parse0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		m_success = codec.readBoolean();
	}

	@Override
	public void toBytes0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		codec.writeBoolean(m_success);
	}

}
