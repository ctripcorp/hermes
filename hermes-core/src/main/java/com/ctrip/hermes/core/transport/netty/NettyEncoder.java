package com.ctrip.hermes.core.transport.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import com.ctrip.hermes.core.transport.command.Command;

public class NettyEncoder extends MessageToByteEncoder<Command> {

	@Override
	protected void encode(ChannelHandlerContext ctx, Command command, ByteBuf out) throws Exception {
		command.toBytes(out);
	}

}
