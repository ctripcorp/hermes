package com.ctrip.hermes.core.message.retry;

public class FixedIntervalRetryPolicy implements RetryPolicy {

	private int m_retryTimes;

	private int m_intervalMillis;

	public FixedIntervalRetryPolicy(int retryTimes, int intervalMillis) {
		m_retryTimes = retryTimes;
		m_intervalMillis = intervalMillis;
	}

	/**
	 * 
	 * @param stringFormat
	 *           [retryTimes,intervalMillis], [3,1000] means retry 3 times every 1000 milliseconds
	 */
	public FixedIntervalRetryPolicy(String stringFormat) {
		try {
			String[] parts = stringFormat.substring(1, stringFormat.length() - 1).split(",");
			if (parts.length != 2) {
				throw new IllegalArgumentException(String.format("Policy value %s is invalid", stringFormat));
			} else {
				m_retryTimes = Integer.parseInt(parts[0].trim());
				m_intervalMillis = Integer.parseInt(parts[1].trim());
			}
		} catch (Exception e) {
			throw new IllegalArgumentException(String.format("Policy value %s is invalid", stringFormat), e);
		}
	}

	public int getIntervalMillis() {
		return m_intervalMillis;
	}

	@Override
	public int getRetryTimes() {
		return m_retryTimes;
	}

	@Override
	public long nextScheduleTimeMillis(int retryTimes, long currentTimeMillis) {
		return currentTimeMillis + m_intervalMillis;
	}

}
