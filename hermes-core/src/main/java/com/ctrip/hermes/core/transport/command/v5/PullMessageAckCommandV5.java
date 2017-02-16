package com.ctrip.hermes.core.transport.command.v5;

import io.netty.buffer.ByteBuf;

import com.ctrip.hermes.core.transport.command.AbstractCommand;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;
import com.ctrip.hermes.meta.entity.Endpoint;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class PullMessageAckCommandV5 extends AbstractCommand {

	private static final long serialVersionUID = -2462726426306841225L;

	private boolean m_success = false;

	private Endpoint m_newEndpoint;

	public PullMessageAckCommandV5() {
		super(CommandType.ACK_MESSAGE_PULL_V5, 5);
	}

	public void setSuccess(boolean success) {
		m_success = success;
	}

	public boolean isSuccess() {
		return m_success;
	}

	public void setNewEndpoint(Endpoint endpoint) {
		m_newEndpoint = endpoint;
	}

	public Endpoint getNewEndpoint() {
		return m_newEndpoint;
	}

	@Override
	public void parse0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		m_success = codec.readBoolean();
		m_newEndpoint = codec.readEndpoint();
	}

	@Override
	public void toBytes0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		codec.writeBoolean(m_success);
		codec.writeEndpoint(m_newEndpoint);
	}

}
