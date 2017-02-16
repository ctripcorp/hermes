package com.ctrip.hermes.consumer.engine.monitor;

import com.ctrip.hermes.core.transport.command.v5.QueryLatestConsumerOffsetCommandV5;
import com.ctrip.hermes.core.transport.command.v5.QueryOffsetResultCommandV5;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface QueryOffsetResultMonitor {
	void monitor(QueryLatestConsumerOffsetCommandV5 cmd);

	void resultReceived(QueryOffsetResultCommandV5 ack);

	void remove(QueryLatestConsumerOffsetCommandV5 cmd);
}
