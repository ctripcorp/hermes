package com.ctrip.hermes.producer.monitor;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.transport.command.v6.SendMessageAckCommandV6;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface SendMessageAcceptanceMonitor {

	public SettableFuture<Pair<Boolean, Endpoint>> monitor(long correlationId);

	public void cancel(long correlationId);

	public void received(SendMessageAckCommandV6 cmd);

}
