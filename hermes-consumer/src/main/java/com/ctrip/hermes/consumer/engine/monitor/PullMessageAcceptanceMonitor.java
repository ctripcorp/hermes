package com.ctrip.hermes.consumer.engine.monitor;

import java.util.concurrent.Future;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.transport.command.v5.PullMessageAckCommandV5;
import com.ctrip.hermes.meta.entity.Endpoint;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface PullMessageAcceptanceMonitor {

	public Future<Pair<Boolean, Endpoint>> monitor(long correlationId);

	public void cancel(long correlationId);

	public void received(PullMessageAckCommandV5 cmd);

}
