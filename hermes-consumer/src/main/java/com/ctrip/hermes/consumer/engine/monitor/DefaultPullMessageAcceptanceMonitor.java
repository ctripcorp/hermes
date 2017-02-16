package com.ctrip.hermes.consumer.engine.monitor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.transport.command.v5.PullMessageAckCommandV5;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = PullMessageAcceptanceMonitor.class)
public class DefaultPullMessageAcceptanceMonitor implements PullMessageAcceptanceMonitor {

	private Map<Long, SettableFuture<Pair<Boolean, Endpoint>>> m_futures = new ConcurrentHashMap<>();

	@Override
	public Future<Pair<Boolean, Endpoint>> monitor(long correlationId) {
		SettableFuture<Pair<Boolean, Endpoint>> future = SettableFuture.create();
		m_futures.put(correlationId, future);
		return future;
	}

	@Override
	public void received(PullMessageAckCommandV5 cmd) {
		SettableFuture<Pair<Boolean, Endpoint>> future = m_futures.remove(cmd.getHeader().getCorrelationId());
		if (future != null) {
			future.set(new Pair<Boolean, Endpoint>(cmd.isSuccess(), cmd.getNewEndpoint()));
		}
	}

	@Override
	public void cancel(long correlationId) {
		m_futures.remove(correlationId);
	}
}
