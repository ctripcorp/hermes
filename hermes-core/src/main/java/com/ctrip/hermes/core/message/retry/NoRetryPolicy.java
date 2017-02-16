package com.ctrip.hermes.core.message.retry;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class NoRetryPolicy implements RetryPolicy {

	@Override
	public int getRetryTimes() {
		return 0;
	}

	@Override
	public long nextScheduleTimeMillis(int retryTimes, long currentTimeMillis) {
		throw new UnsupportedOperationException();
	}

}
