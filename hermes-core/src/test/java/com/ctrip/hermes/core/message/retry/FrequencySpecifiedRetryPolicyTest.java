package com.ctrip.hermes.core.message.retry;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class FrequencySpecifiedRetryPolicyTest {
	@Test(expected = IllegalArgumentException.class)
	public void testInputInvalid1() throws Exception {
		new FrequencySpecifiedRetryPolicy(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInputInvalid2() throws Exception {
		new FrequencySpecifiedRetryPolicy("[1,");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInputInvalid3() throws Exception {
		new FrequencySpecifiedRetryPolicy("1,]");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInputInvalid4() throws Exception {
		new FrequencySpecifiedRetryPolicy("1,");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInputInvalid5() throws Exception {
		new FrequencySpecifiedRetryPolicy("[1a,]");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInputInvalid6() throws Exception {
		new FrequencySpecifiedRetryPolicy("");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInputInvalid7() throws Exception {
		new FrequencySpecifiedRetryPolicy("[]");
	}

	@Test
	public void testValid() throws Exception {
		FrequencySpecifiedRetryPolicy policy = new FrequencySpecifiedRetryPolicy("[1,2]");
		assertEquals(2, policy.getRetryTimes());
		long now = System.currentTimeMillis();

		assertEquals(now + 1000, policy.nextScheduleTimeMillis(0, now));
		assertEquals(now + 3000, policy.nextScheduleTimeMillis(1, now + 1000));
		assertEquals(0, policy.nextScheduleTimeMillis(2, now + 3000));
	}
}
