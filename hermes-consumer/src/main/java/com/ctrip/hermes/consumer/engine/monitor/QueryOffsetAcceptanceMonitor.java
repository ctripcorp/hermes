package com.ctrip.hermes.consumer.engine.monitor;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.transport.command.v5.QueryLatestConsumerOffsetAckCommandV5;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface QueryOffsetAcceptanceMonitor {

	public SettableFuture<Pair<Boolean, Endpoint>> monitor(long correlationId);

	public void cancel(long correlationId);

	public void received(QueryLatestConsumerOffsetAckCommandV5 cmd);

}
