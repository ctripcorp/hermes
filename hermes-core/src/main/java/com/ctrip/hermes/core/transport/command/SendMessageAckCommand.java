package com.ctrip.hermes.core.transport.command;

import io.netty.buffer.ByteBuf;

import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class SendMessageAckCommand extends AbstractCommand {

	private static final long serialVersionUID = -2462726426306841225L;

	private boolean m_success = false;

	public SendMessageAckCommand() {
		super(CommandType.ACK_MESSAGE_SEND, 1);
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
