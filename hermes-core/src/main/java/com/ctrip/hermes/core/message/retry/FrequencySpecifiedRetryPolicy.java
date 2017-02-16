package com.ctrip.hermes.core.message.retry;

import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class FrequencySpecifiedRetryPolicy implements RetryPolicy {
	private static final Pattern PATTERN_VALID = Pattern.compile("\\[(\\d+,*)+\\]");

	private static final Pattern PATTERN_GROUP = Pattern.compile("(\\d+),*");

	private String m_policyValue;

	private int m_retryTimes;

	private List<Integer> m_intervals;

	public FrequencySpecifiedRetryPolicy(String policyValue) {
		if (policyValue == null) {
			throw new IllegalArgumentException("Policy value can not be null");
		}

		m_policyValue = policyValue.trim();

		if (PATTERN_VALID.matcher(m_policyValue).matches()) {
			m_intervals = new LinkedList<Integer>();
			Matcher m = PATTERN_GROUP.matcher(m_policyValue.substring(1, m_policyValue.length() - 1));
			while (m.find()) {
				m_intervals.add(Integer.valueOf(m.group(1)));
			}
			m_retryTimes = m_intervals.size();
		} else {
			throw new IllegalArgumentException(String.format("Policy value %s is invalid", m_policyValue));
		}
	}

	@Override
	public int getRetryTimes() {
		return m_retryTimes;
	}

	@Override
	public long nextScheduleTimeMillis(int retryTimes, long currentTimeMillis) {
		if (retryTimes >= m_retryTimes) {
			return 0L;
		} else {
			return currentTimeMillis + m_intervals.get(retryTimes) * 1000;
		}
	}
}
