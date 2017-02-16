package com.ctrip.hermes.broker.shutdown;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.broker.longpolling.LongPollingService;
import com.ctrip.hermes.broker.queue.MessageQueueManager;
import com.ctrip.hermes.broker.transport.NettyServer;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorManager;
import com.ctrip.hermes.core.transport.endpoint.EndpointClient;
import com.ctrip.hermes.core.transport.netty.NettyUtils;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.env.config.broker.BrokerConfigProvider;
import com.google.common.base.Charsets;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = ShutdownRequestMonitor.class)
public class ShutdownRequestMonitor {
	private static final Logger log = LoggerFactory.getLogger(ShutdownRequestMonitor.class);

	private static final int MAX_IDLE_SECONDS = 30;

	@Inject
	private CommandProcessorManager m_commandProcessor;

	@Inject
	private MessageQueueManager m_messageQueueManager;

	@Inject
	private LongPollingService m_longPollingService;

	@Inject
	private NettyServer m_nettyServer;

	@Inject
	private BrokerConfigProvider m_config;

	@Inject
	private EndpointClient m_endpointClient;

	private EventLoopGroup m_bossGroup = new NioEventLoopGroup(0, HermesThreadFactory.create("ShutdownMonitor-boss",
	      false));

	private EventLoopGroup m_workerGroup = new NioEventLoopGroup(0, HermesThreadFactory.create("ShutdownMonitor-worker",
	      false));

	public void start() {
		ServerBootstrap b = new ServerBootstrap();

		b.group(m_bossGroup, m_workerGroup)//
		      .channel(NioServerSocketChannel.class)//
		      .childHandler(new ChannelInitializer<SocketChannel>() {
			      @Override
			      public void initChannel(SocketChannel ch) throws Exception {
				      ch.pipeline().addLast(new LineBasedFrameDecoder(10),//
				            new StringDecoder(Charsets.UTF_8),//
				            new IdleStateHandler(0, 0, MAX_IDLE_SECONDS),//
				            new ShutdownRequestInboundHandler());
			      }
		      }).option(ChannelOption.SO_BACKLOG, 128) // TODO set tcp options
		      .childOption(ChannelOption.SO_KEEPALIVE, true);

		// Bind and start to accept incoming connections.
		ChannelFuture f = b.bind(m_config.getShutdownRequestPort());

		f.addListener(new ChannelFutureListener() {

			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				if (future.isSuccess()) {
					log.info("Broker shutdown port is {}.", m_config.getShutdownRequestPort());
				} else {
					log.error("Failed to listen shutdown port {}.", m_config.getShutdownRequestPort());
				}

			}
		});
	}

	public void stopBroker() {
		m_commandProcessor.stop();
		m_longPollingService.stop();
		m_messageQueueManager.stop();
		m_endpointClient.close();
		m_nettyServer.stop();
		m_bossGroup.shutdownGracefully();
		m_workerGroup.shutdownGracefully();
	}

	private class ShutdownRequestInboundHandler extends SimpleChannelInboundHandler<String> {

		@Override
		protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
			if ("shutdown".equals(msg)) {
				stopBroker();
			} else {
				ctx.channel().close();
			}
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
			log.error("Exception occurred in netty channel, will close channel(addr={})",
			      NettyUtils.parseChannelRemoteAddr(ctx.channel()), cause);
			ctx.channel().close();
		}

		@Override
		public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
			if (evt instanceof IdleStateEvent) {
				IdleStateEvent evnet = (IdleStateEvent) evt;
				if (evnet.state().equals(IdleState.ALL_IDLE)) {
					log.info("Client idle for {} seconds, will remove it automatically(client addr={})", MAX_IDLE_SECONDS,
					      NettyUtils.parseChannelRemoteAddr(ctx.channel()));
					ctx.channel().close();
				}
			}
			super.userEventTriggered(ctx, evt);
		}

	}

}
