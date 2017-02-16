package com.ctrip.hermes.core.transport.endpoint;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.core.config.CoreConfig;
import com.ctrip.hermes.core.constants.CatConstants;
import com.ctrip.hermes.core.transport.command.Command;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorManager;
import com.ctrip.hermes.core.transport.netty.DefaultNettyChannelOutboundHandler;
import com.ctrip.hermes.core.transport.netty.MagicNumberAndLengthPrepender;
import com.ctrip.hermes.core.transport.netty.NettyDecoder;
import com.ctrip.hermes.core.transport.netty.NettyEncoder;
import com.ctrip.hermes.core.utils.CatUtil;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.meta.entity.Endpoint;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public abstract class AbstractEndpointClient implements EndpointClient, Initializable {
	private static final Logger log = LoggerFactory.getLogger(AbstractEndpointClient.class);

	private final ConcurrentMap<Endpoint, EndpointChannel> m_channels = new ConcurrentHashMap<Endpoint, EndpointChannel>();

	private EventLoopGroup m_eventLoopGroup;

	@Inject
	private CoreConfig m_config;

	@Inject
	private CommandProcessorManager m_commandProcessorManager;

	private AtomicBoolean m_closed = new AtomicBoolean(false);

	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

	@Override
	public boolean writeCommand(Endpoint endpoint, Command cmd) {
		if (m_closed.get()) {
			return false;
		}

		CommandType type = cmd.getHeader().getType();
		if (type != null) {
			CatUtil.logEventPeriodically(CatConstants.TYPE_HERMES_CMD_VERSION, type + "-SEND");
		}

		lock.readLock().lock();
		try {
			return getChannel(endpoint).write(cmd);
		} finally {
			lock.readLock().unlock();
		}
	}

	private EndpointChannel getChannel(Endpoint endpoint) {
		if (Endpoint.BROKER.equalsIgnoreCase(endpoint.getType())) {
			EndpointChannel channel = m_channels.get(endpoint);

			if (channel == null) {
				synchronized (m_channels) {
					channel = m_channels.get(endpoint);
					if (channel == null) {
						channel = creatChannel(endpoint);
						m_channels.put(endpoint, channel);
					}
				}
			}

			return channel;
		} else {
			throw new IllegalArgumentException(String.format("Unknown endpoint type: %s", endpoint.getType()));
		}
	}

	void removeChannel(Endpoint endpoint, EndpointChannel endpointChannel, boolean sinceIdle) {
		lock.writeLock().lock();
		try {
			EndpointChannel removedChannel = null;
			if (Endpoint.BROKER.equals(endpoint.getType()) && m_channels.containsKey(endpoint)) {
				if (m_channels.containsKey(endpoint)) {
					EndpointChannel tmp = m_channels.get(endpoint);
					if (tmp == endpointChannel) {
						if (!sinceIdle) {
							m_channels.remove(endpoint);
							removedChannel = endpointChannel;
						}
					}
				}
			}

			if (removedChannel != null) {
				log.info("Closing connection to broker({}:{}, endpointId={}, sinceIdle:{})", endpoint.getHost(),
				      endpoint.getPort(), endpoint.getId(), sinceIdle);
				removedChannel.close();
			}
		} finally {
			lock.writeLock().unlock();
		}
	}

	private EndpointChannel creatChannel(Endpoint endpoint) {
		EndpointChannel endpointChannel = new EndpointChannel();
		connect(endpoint, endpointChannel);
		return endpointChannel;
	}

	protected abstract boolean isEndpointValid(Endpoint endpoint);

	void connect(final Endpoint endpoint, final EndpointChannel endpointChannel) {
		ChannelFuture channelFuture = createBootstrap(endpoint, endpointChannel).connect(endpoint.getHost(),
		      endpoint.getPort());

		channelFuture.addListener(new ChannelFutureListener() {

			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				if (!endpointChannel.isClosed()) {
					if (!future.isSuccess()) {
						endpointChannel.setChannelFuture(null);

						if (isEndpointValid(endpoint)) {
							final EventLoop loop = future.channel().eventLoop();
							loop.schedule(new Runnable() {
								@Override
								public void run() {
									log.info("Reconnecting to broker({}:{}, endpointId={})", endpoint.getHost(),
									      endpoint.getPort(), endpoint.getId());
									connect(endpoint, endpointChannel);
								}
							}, m_config.getEndpointChannelAutoReconnectDelay(), TimeUnit.SECONDS);
						} else {
							removeChannel(endpoint, endpointChannel, false);
						}
					} else {
						endpointChannel.setChannelFuture(future);
					}
				} else {
					if (future.isSuccess()) {
						future.channel().close();
					}
				}
			}

		});
	}

	@Override
	public void initialize() throws InitializationException {
		m_eventLoopGroup = new NioEventLoopGroup(0, HermesThreadFactory.create("NettyWriterEventLoop", false));
	}

	@Override
	public void close() {
		if (m_closed.compareAndSet(false, true)) {
			lock.writeLock().lock();
			try {
				for (Map.Entry<Endpoint, EndpointChannel> entry : m_channels.entrySet()) {
					removeChannel(entry.getKey(), entry.getValue(), false);
				}
			} finally {
				lock.writeLock().unlock();
			}

			m_eventLoopGroup.shutdownGracefully();
		}
	}

	private Bootstrap createBootstrap(final Endpoint endpoint, final EndpointChannel endpointChannel) {
		Bootstrap bootstrap = new Bootstrap();
		bootstrap.group(m_eventLoopGroup);
		bootstrap.channel(NioSocketChannel.class);
		bootstrap.option(ChannelOption.SO_KEEPALIVE, true)//
		      .option(ChannelOption.TCP_NODELAY, true)//
		      .option(ChannelOption.SO_SNDBUF, m_config.getNettySendBufferSize())//
		      .option(ChannelOption.SO_RCVBUF, m_config.getNettyReceiveBufferSize());

		bootstrap.handler(new ChannelInitializer<SocketChannel>() {
			@Override
			public void initChannel(SocketChannel ch) throws Exception {

				ch.pipeline().addLast(
				      //
				      new DefaultNettyChannelOutboundHandler(), //
				      new NettyDecoder(), //
				      new MagicNumberAndLengthPrepender(), //
				      new NettyEncoder(), //
				      new IdleStateHandler(m_config.getEndpointChannelReadIdleTime(), //
				            m_config.getEndpointChannelWriteIdleTime(), //
				            m_config.getEndpointChannelMaxIdleTime()), //
				      new DefaultClientChannelInboundHandler(m_commandProcessorManager, endpoint, endpointChannel,
				            AbstractEndpointClient.this, m_config));
			}
		});

		return bootstrap;
	}

	class EndpointChannel {

		private AtomicReference<ChannelFuture> m_channelFuture = new AtomicReference<ChannelFuture>(null);

		private AtomicBoolean m_closed = new AtomicBoolean(false);

		public void setChannelFuture(ChannelFuture channelFuture) {
			if (!isClosed()) {
				m_channelFuture.set(channelFuture);
			}
		}

		public boolean isClosed() {
			return m_closed.get();
		}

		public void close() {
			if (m_closed.compareAndSet(false, true)) {
				ChannelFuture channelFuture = m_channelFuture.get();
				if (channelFuture != null) {
					channelFuture.channel().close();
				}
			}
		}

		public boolean write(Command cmd) {
			if (!isClosed()) {
				ChannelFuture channelFuture = m_channelFuture.get();
				Channel channel = null;
				if (channelFuture != null) {
					channel = channelFuture.channel();
					if (channel.isActive() && channel.isWritable()) {
						channel.writeAndFlush(cmd);
						return true;
					}
				}
			}

			return false;
		}
	}
}
