package com.ctrip.hermes.core.transport.endpoint;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.core.constants.CatConstants;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorManager;
import com.ctrip.hermes.core.transport.netty.AbstractNettyChannelInboundHandler;
import com.ctrip.hermes.core.transport.netty.NettyUtils;
import com.dianping.cat.Cat;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class DefaultServerChannelInboundHandler extends AbstractNettyChannelInboundHandler {
	private static final Logger log = LoggerFactory.getLogger(DefaultServerChannelInboundHandler.class);

	private int m_maxIdleTime;

	public DefaultServerChannelInboundHandler(CommandProcessorManager cmdProcessorManager, int maxIdleTime) {
		super(cmdProcessorManager);
		m_maxIdleTime = maxIdleTime;
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		log.info("Client connected(addr={})", NettyUtils.parseChannelRemoteAddr(ctx.channel()));
		super.channelActive(ctx);
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		log.info("Client disconnected(addr={})", NettyUtils.parseChannelRemoteAddr(ctx.channel()));
		super.channelInactive(ctx);
	}

	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		log.warn("Exception caught in inbound, client ip {}", NettyUtils.parseChannelRemoteAddr(ctx.channel()), cause);
		Cat.logEvent(CatConstants.TYPE_SERVER_INBOUND_ERROR, NettyUtils.parseChannelRemoteAddr(ctx.channel(), false));

		ctx.channel().close();
	}

	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
		if (evt instanceof IdleStateEvent) {
			IdleStateEvent evnet = (IdleStateEvent) evt;
			if (evnet.state().equals(IdleState.ALL_IDLE)) {
				log.info("Client idle for {} seconds, will remove it automatically(client addr={})", m_maxIdleTime,
				      NettyUtils.parseChannelRemoteAddr(ctx.channel()));
				ctx.channel().close();
			}
		}
		super.userEventTriggered(ctx, evt);
	}
}
