package com.ctrip.hermes.broker.longpolling;


/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface LongPollingService {

	void schedulePush(PullMessageTask task);

	void stop();
}
