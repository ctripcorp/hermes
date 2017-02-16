package com.ctrip.hermes.core.transport.endpoint;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;
import io.netty.handler.timeout.IdleStateEvent;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.core.config.CoreConfig;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorManager;
import com.ctrip.hermes.core.transport.endpoint.AbstractEndpointClient.EndpointChannel;
import com.ctrip.hermes.core.transport.netty.AbstractNettyChannelInboundHandler;
import com.ctrip.hermes.core.transport.netty.NettyUtils;
import com.ctrip.hermes.meta.entity.Endpoint;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class DefaultClientChannelInboundHandler extends AbstractNettyChannelInboundHandler {
	private static final Logger log = LoggerFactory.getLogger(DefaultClientChannelInboundHandler.class);

	private EndpointChannel m_endpointChannel;

	private Endpoint m_endpoint;

	private AbstractEndpointClient m_endpointClient;

	private CoreConfig m_config;

	public DefaultClientChannelInboundHandler(CommandProcessorManager cmdProcessorManager, Endpoint endpoint,
	      EndpointChannel endpointChannel, AbstractEndpointClient endpointClient, CoreConfig config) {
		super(cmdProcessorManager);
		m_endpoint = endpoint;
		m_endpointChannel = endpointChannel;
		m_config = config;
		m_endpointClient = endpointClient;
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		log.info("Connected to broker(addr={})", NettyUtils.parseChannelRemoteAddr(ctx.channel()));
		super.channelActive(ctx);
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		log.info("Disconnected from broker(addr={})", NettyUtils.parseChannelRemoteAddr(ctx.channel()));

		if (!m_endpointChannel.isClosed()) {
			m_endpointChannel.setChannelFuture(null);
			final EventLoop loop = ctx.channel().eventLoop();
			loop.schedule(new Runnable() {
				@Override
				public void run() {
					log.info("Reconnecting to broker({}:{})", m_endpoint.getHost(), m_endpoint.getPort());
					m_endpointClient.connect(m_endpoint, m_endpointChannel);
				}
			}, m_config.getEndpointChannelAutoReconnectDelay(), TimeUnit.SECONDS);
		}
		super.channelInactive(ctx);
	}

	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
		if (evt instanceof IdleStateEvent) {
			IdleStateEvent e = (IdleStateEvent) evt;
			switch (e.state()) {
			case ALL_IDLE:
			case READER_IDLE:
			case WRITER_IDLE:
				m_endpointClient.removeChannel(m_endpoint, m_endpointChannel, true);
				break;
			}
		}
	}

}
