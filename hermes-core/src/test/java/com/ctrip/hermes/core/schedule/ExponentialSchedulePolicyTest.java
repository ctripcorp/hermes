package com.ctrip.hermes.core.schedule;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class ExponentialSchedulePolicyTest {
	@Test
	public void testFail() {
		ExponentialSchedulePolicy policy = new ExponentialSchedulePolicy(10, 80);
		assertEquals(10, policy.fail(false));
		assertEquals(20, policy.fail(false));
		assertEquals(40, policy.fail(false));
		assertEquals(80, policy.fail(false));
		for (int i = 0; i < 100; i++) {
			assertEquals(80, policy.fail(false));
		}
	}

	@Test
	public void testFailAndSuccess() {
		ExponentialSchedulePolicy policy = new ExponentialSchedulePolicy(10, 80);
		assertEquals(10, policy.fail(false));
		assertEquals(20, policy.fail(false));
		assertEquals(40, policy.fail(false));
		policy.succeess();
		assertEquals(10, policy.fail(false));
		assertEquals(20, policy.fail(false));
		assertEquals(40, policy.fail(false));
		assertEquals(80, policy.fail(false));
		for (int i = 0; i < 100; i++) {
			assertEquals(80, policy.fail(false));
		}
		policy.succeess();
		assertEquals(10, policy.fail(false));
		assertEquals(20, policy.fail(false));
		assertEquals(40, policy.fail(false));
		assertEquals(80, policy.fail(false));
	}
}
