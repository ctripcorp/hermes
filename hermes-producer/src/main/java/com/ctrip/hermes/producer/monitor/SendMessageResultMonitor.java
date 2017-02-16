package com.ctrip.hermes.producer.monitor;

import java.util.concurrent.Future;

import com.ctrip.hermes.core.bo.SendMessageResult;
import com.ctrip.hermes.core.transport.command.v6.SendMessageCommandV6;
import com.ctrip.hermes.core.transport.command.v6.SendMessageResultCommandV6;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface SendMessageResultMonitor {

	Future<SendMessageResult> monitor(SendMessageCommandV6 cmd);

	void cancel(SendMessageCommandV6 cmd);

	void resultReceived(SendMessageResultCommandV6 result);

}
