package com.ctrip.hermes.metaserver.monitor;

import com.ctrip.hermes.core.transport.command.QueryMessageOffsetByTimeCommand;
import com.ctrip.hermes.core.transport.command.QueryOffsetResultCommand;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface QueryOffsetResultMonitor {

	void monitor(QueryMessageOffsetByTimeCommand cmd);

	void resultReceived(QueryOffsetResultCommand ack);

	void remove(QueryMessageOffsetByTimeCommand cmd);

}
