package com.ctrip.hermes.core.message.retry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

public class FixedIntervalRetryPolicyTest {

	@Test
	public void testInvalidInput() {
		assertException(null);
		assertException("");
		assertException("a");
		assertException("[]");
		assertException("[1]");
		assertException("[1,]");
	}

	@Test
	public void testValidFormat() {
		assertOK("[1,100]", 1, 100);
		assertOK("[ 1 , 100 ]", 1, 100);
		assertOK("[100,5000]", 100, 5000);
	}

	private void assertOK(String stringFormat, int retryTimes, int interval) {
		try {
			FixedIntervalRetryPolicy p = new FixedIntervalRetryPolicy(stringFormat);
			assertEquals(retryTimes, p.getRetryTimes());
			assertEquals(interval, p.getIntervalMillis());
			for (int i = 0; i < retryTimes; i++) {
				assertEquals(interval, p.nextScheduleTimeMillis(i, 0));
			}
		} catch (Exception e) {
			fail();
		}
	}

	private void assertException(String stringFormat) {
		try {
			new FixedIntervalRetryPolicy(stringFormat);
			fail();
		} catch (Exception e) {
		}
	}

}
