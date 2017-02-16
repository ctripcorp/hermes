package com.ctrip.hermes.core.message.retry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class RetryPolicyFactoryTest {

	@Test(expected = IllegalArgumentException.class)
	public void testInvalid1() throws Exception {
		RetryPolicyFactory.create(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalid2() throws Exception {
		RetryPolicyFactory.create("2");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalid3() throws Exception {
		RetryPolicyFactory.create("2:");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalid4() throws Exception {
		RetryPolicyFactory.create("2:  ");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalid5() throws Exception {
		RetryPolicyFactory.create("   :1");
	}

	@Test
	public void testValid() throws Exception {
		RetryPolicy policy = RetryPolicyFactory.create("1:[1,2]");
		assertTrue(policy instanceof FrequencySpecifiedRetryPolicy);
		assertEquals(2, policy.getRetryTimes());
	}

	@Test
	public void testValid2() throws Exception {
		RetryPolicy policy = RetryPolicyFactory.create("3:[10,5000]");
		assertTrue(policy instanceof FixedIntervalRetryPolicy);
		assertEquals(10, policy.getRetryTimes());
		assertEquals(5000, ((FixedIntervalRetryPolicy) policy).getIntervalMillis());
	}
}
