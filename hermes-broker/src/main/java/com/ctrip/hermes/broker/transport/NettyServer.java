package com.ctrip.hermes.broker.transport;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;

import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.transport.command.processor.CommandProcessorManager;
import com.ctrip.hermes.core.transport.endpoint.DefaultServerChannelInboundHandler;
import com.ctrip.hermes.core.transport.netty.DefaultNettyChannelOutboundHandler;
import com.ctrip.hermes.core.transport.netty.MagicNumberAndLengthPrepender;
import com.ctrip.hermes.core.transport.netty.NettyDecoder;
import com.ctrip.hermes.core.transport.netty.NettyEncoder;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.env.config.broker.BrokerConfigProvider;

@Named(type = NettyServer.class)
public class NettyServer extends ContainerHolder {

	@Inject
	private BrokerConfigProvider m_config;

	private EventLoopGroup m_bossGroup = new NioEventLoopGroup(0, HermesThreadFactory.create("NettyServer-boss", false));

	private EventLoopGroup m_workerGroup = new NioEventLoopGroup(0, HermesThreadFactory.create("NettyServer-worker",
	      false));

	public ChannelFuture start(int port) {
		ServerBootstrap b = new ServerBootstrap();
		b.group(m_bossGroup, m_workerGroup)//
		      .channel(NioServerSocketChannel.class)//
		      .childHandler(new ChannelInitializer<SocketChannel>() {
			      @Override
			      public void initChannel(SocketChannel ch) throws Exception {
				      ch.pipeline().addLast(
				            new DefaultNettyChannelOutboundHandler(),//
				            new NettyDecoder(), //
				            new MagicNumberAndLengthPrepender(), //
				            new NettyEncoder(), //
				            new IdleStateHandler(0, 0, m_config.getClientMaxIdleSeconds()),//
				            new DefaultServerChannelInboundHandler(lookup(CommandProcessorManager.class), m_config
				                  .getClientMaxIdleSeconds()));
			      }
		      }).option(ChannelOption.SO_BACKLOG, 128) // TODO set tcp options
		      .childOption(ChannelOption.SO_KEEPALIVE, true)//
		      .childOption(ChannelOption.SO_REUSEADDR, true)//
		      .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)//
		      .childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, 32 * 1024)//
		      .childOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 8 * 1024)//
		;

		// Bind and start to accept incoming connections.
		ChannelFuture f = b.bind(port);

		return f;
	}

	public void stop() {
		m_workerGroup.shutdownGracefully();
		m_bossGroup.shutdownGracefully();
	}

}
