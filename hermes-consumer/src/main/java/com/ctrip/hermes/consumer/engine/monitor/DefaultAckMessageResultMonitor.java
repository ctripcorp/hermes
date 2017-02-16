package com.ctrip.hermes.consumer.engine.monitor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.transport.command.v5.AckMessageResultCommandV5;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = AckMessageResultMonitor.class)
public class DefaultAckMessageResultMonitor implements AckMessageResultMonitor {

	private Map<Long, SettableFuture<Boolean>> m_futures = new ConcurrentHashMap<Long, SettableFuture<Boolean>>();

	@Override
	public Future<Boolean> monitor(long correlationId) {
		SettableFuture<Boolean> future = SettableFuture.create();
		m_futures.put(correlationId, future);
		return future;
	}

	@Override
	public void received(AckMessageResultCommandV5 cmd) {

		SettableFuture<Boolean> future = m_futures.remove(cmd.getHeader().getCorrelationId());
		if (future != null) {
			future.set(cmd.isSuccess());
		}
	}

	@Override
	public void cancel(long correlationId) {
		m_futures.remove(correlationId);
	}
}
