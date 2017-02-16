package com.ctrip.hermes.consumer.integration.assist;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;

public enum LeaseAnswer implements Answer<LeaseAcquireResponse> {
	SUCCESS() {
		@Override
		public LeaseAcquireResponse answer(InvocationOnMock invocation) throws Throwable {
			long expireTime = System.currentTimeMillis() + m_durationMill;
			long leaseId = m_leaseId.incrementAndGet();
			waitUntilTrigger();
			return new LeaseAcquireResponse(true, new Lease(leaseId, expireTime), expireTime);
		}
	},
	FAILED() {
		@Override
		public LeaseAcquireResponse answer(InvocationOnMock invocation) throws Throwable {
			waitUntilTrigger();
			return new LeaseAcquireResponse(false, null, System.currentTimeMillis() + m_durationMill);
		}
	},
	FAILED_NULL() {
		@Override
		public LeaseAcquireResponse answer(InvocationOnMock invocation) throws Throwable {
			return null;
		}
	};
	public static final int DEFAULT_DURATION_MILLS = 200;

	private static AtomicLong m_leaseId = new AtomicLong(0);

	private static int m_durationMill = DEFAULT_DURATION_MILLS;

	private static int m_answerDelay = 0;

	public LeaseAnswer durationInMillisecond(int durationMill) {
		m_durationMill = durationMill;
		return this;
	}

	public LeaseAnswer withDelay(int delay) {
		m_answerDelay = delay;
		return this;
	}

	private static void waitUntilTrigger() {
		if (m_answerDelay > 0) {
			try {
				TimeUnit.MILLISECONDS.sleep(m_answerDelay);
			} catch (InterruptedException e) {
				// ignore
			}
		}
	}

	public static void reset() {
		m_answerDelay = 0;
	}
}
