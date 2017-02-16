package com.ctrip.hermes.core.transport.command;

import io.netty.buffer.ByteBuf;

import java.io.Serializable;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface Command extends Serializable {
	public Header getHeader();

	public void parse(ByteBuf buf, Header header);

	public void toBytes(ByteBuf buf);

	public void release();

	public long getReceiveTime();

}
