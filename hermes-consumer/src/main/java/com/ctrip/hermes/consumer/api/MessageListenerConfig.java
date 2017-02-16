package com.ctrip.hermes.consumer.api;

import com.ctrip.hermes.core.message.retry.RetryPolicy;

public class MessageListenerConfig {

	private RetryPolicy m_strictlyOrderingRetryPolicy;

	private boolean m_strictlyOrdering = false;

	public void setStrictlyOrderingRetryPolicy(StrictlyOrderingRetryPolicy policy) {
		m_strictlyOrderingRetryPolicy = policy.toRetryPolicy();
		m_strictlyOrdering = true;
	}

	public boolean isStrictlyOrdering() {
		return m_strictlyOrdering;
	}

	public RetryPolicy getStrictlyOrderingRetryPolicy() {
		return m_strictlyOrderingRetryPolicy;
	}

	public static abstract class StrictlyOrderingRetryPolicy {
		private StrictlyOrderingRetryPolicy() {
		}

		public static StrictlyOrderingRetryPolicy evenRetry(int retryIntervalMills, int retryTimes) {
			return new EvenRetryPolicy(retryIntervalMills, retryTimes);
		}

		protected abstract RetryPolicy toRetryPolicy();

		static class EvenRetryPolicy extends StrictlyOrderingRetryPolicy {

			private int m_retryIntervalMills;

			private int m_retryTimes;

			public EvenRetryPolicy(int retryIntervalMills, int retryTimes) {
				m_retryIntervalMills = retryIntervalMills;
				m_retryTimes = retryTimes;
			}

			@Override
			protected RetryPolicy toRetryPolicy() {
				return new RetryPolicy() {

					@Override
					public long nextScheduleTimeMillis(int retryTimes, long currentTimeMillis) {
						return currentTimeMillis + m_retryIntervalMills;
					}

					@Override
					public int getRetryTimes() {
						return m_retryTimes;
					}
				};
			}

		}
	}

}
