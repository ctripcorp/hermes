package com.ctrip.hermes.consumer.engine.monitor;

import com.ctrip.hermes.core.transport.command.PullMessageResultListener;
import com.ctrip.hermes.core.transport.command.v5.PullMessageResultCommandV5;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface PullMessageResultMonitor {

	void monitor(PullMessageResultListener cmd);

	void resultReceived(PullMessageResultCommandV5 ack);

	void remove(PullMessageResultListener cmd);

}
