package com.ctrip.hermes.core.message.retry;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class DefaultRetryPolicy implements RetryPolicy {

	@Override
	public int getRetryTimes() {
		return 3;
	}

	@Override
	public long nextScheduleTimeMillis(int retryTimes, long currentTimeMillis) {
		return currentTimeMillis + 15000;
	}

}
