package com.ctrip.hermes.core.message.retry;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class RetryPolicyFactory {

	private static final int cacheSize = 100;

	private static Map<String, RetryPolicy> m_cache = new LinkedHashMap<String, RetryPolicy>(
	      (int) Math.ceil(cacheSize / 0.75f) + 1, 0.75f, true) {
		private static final long serialVersionUID = 1L;

		@Override
		protected boolean removeEldestEntry(Map.Entry<String, RetryPolicy> eldest) {
			return size() > cacheSize;
		}
	};

	public static RetryPolicy create(String policyValue) {
		if (policyValue != null) {
			RetryPolicy policy = m_cache.get(policyValue);
			if (policy == null) {
				synchronized (m_cache) {
					policy = m_cache.get(policyValue);
					if (policy == null) {
						policy = createPolicy(policyValue);
						m_cache.put(policyValue, policy);
					}
				}
			}
			if (policy != null) {
				return policy;
			}
		}

		throw new IllegalArgumentException(String.format("Unknown retry policy for value %s", policyValue));
	}

	private static RetryPolicy createPolicy(String policyValue) {
		if (policyValue.indexOf(":") != -1) {
			String[] splits = policyValue.split(":");
			if (splits != null && splits.length == 2) {
				String type = splits[0];
				String value = splits[1];

				if (type != null && !"".equals(type.trim()) && value != null && !"".equals(value.trim())) {

					if ("1".equals(type.trim())) {
						return new FrequencySpecifiedRetryPolicy(value.trim());
					} else if ("2".equals(type.trim()) && "[]".equals(value.trim())) {
						return new NoRetryPolicy();
					} else if ("3".equals(type.trim())) {
						return new FixedIntervalRetryPolicy(value.trim());
					}
				}
			}
		}

		return null;
	}
}
