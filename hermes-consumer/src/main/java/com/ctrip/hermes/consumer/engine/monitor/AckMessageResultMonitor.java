package com.ctrip.hermes.consumer.engine.monitor;

import java.util.concurrent.Future;

import com.ctrip.hermes.core.transport.command.v5.AckMessageResultCommandV5;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface AckMessageResultMonitor {

	Future<Boolean> monitor(long correlationId);

	void received(AckMessageResultCommandV5 cmd);

	void cancel(long correlationId);

}
