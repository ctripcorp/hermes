package com.ctrip.hermes.core.message.retry;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface RetryPolicy {

	public int getRetryTimes();

	public long nextScheduleTimeMillis(int retryTimes, long currentTimeMillis);
}
