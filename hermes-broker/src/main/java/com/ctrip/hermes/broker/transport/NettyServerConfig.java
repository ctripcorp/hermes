package com.ctrip.hermes.broker.transport;

import org.unidal.lookup.annotation.Named;

@Named(type = NettyServerConfig.class)
public class NettyServerConfig {
	private int listenPort = 4376;

	private int serverWorkerThreads = 8;

	private int serverCallbackExecutorThreads = 0;

	private int serverSelectorThreads = 3;

	private int serverOnewaySemaphoreValue = 256;

	private int serverAsyncSemaphoreValue = 64;

	private int serverChannelMaxIdleTimeSeconds = 120;

	private int serverSocketSndBufSize;

	private int serverSocketRcvBufSize;

	private boolean serverPooledByteBufAllocatorEnable = false;

	public int getListenPort() {
		return listenPort;
	}

	public int getServerWorkerThreads() {
		return serverWorkerThreads;
	}

	public int getServerCallbackExecutorThreads() {
		return serverCallbackExecutorThreads;
	}

	public int getServerSelectorThreads() {
		return serverSelectorThreads;
	}

	public int getServerOnewaySemaphoreValue() {
		return serverOnewaySemaphoreValue;
	}

	public int getServerAsyncSemaphoreValue() {
		return serverAsyncSemaphoreValue;
	}

	public int getServerChannelMaxIdleTimeSeconds() {
		return serverChannelMaxIdleTimeSeconds;
	}

	public int getServerSocketSndBufSize() {
		return serverSocketSndBufSize;
	}

	public int getServerSocketRcvBufSize() {
		return serverSocketRcvBufSize;
	}

	public boolean isServerPooledByteBufAllocatorEnable() {
		return serverPooledByteBufAllocatorEnable;
	}

}
