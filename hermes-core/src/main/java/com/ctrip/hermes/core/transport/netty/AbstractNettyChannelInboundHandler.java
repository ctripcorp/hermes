package com.ctrip.hermes.core.transport.netty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import com.ctrip.hermes.core.transport.command.Command;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorManager;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public abstract class AbstractNettyChannelInboundHandler extends SimpleChannelInboundHandler<Command> {

	private static final Logger log = LoggerFactory.getLogger(AbstractNettyChannelInboundHandler.class);

	protected CommandProcessorManager m_cmdProcessorManager;

	public AbstractNettyChannelInboundHandler(CommandProcessorManager cmdProcessorManager) {
		m_cmdProcessorManager = cmdProcessorManager;
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Command cmd) throws Exception {
		m_cmdProcessorManager.offer(new CommandProcessorContext(cmd, ctx.channel()));
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		log.error("Exception occurred in netty channel, will close channel(addr={})",
		      NettyUtils.parseChannelRemoteAddr(ctx.channel()), cause);
		ctx.channel().close();
	}

}
