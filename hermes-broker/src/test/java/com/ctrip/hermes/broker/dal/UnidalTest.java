package com.ctrip.hermes.broker.dal;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

public class UnidalTest extends ComponentTestCase {

	public static class DynamicAddedComponent {

	}

	@Test
	public void test() throws Exception {
		try {
			lookup(DynamicAddedComponent.class, "role");
			fail();
		} catch (Exception e) {
		}

		defineComponent(DynamicAddedComponent.class, "role", DynamicAddedComponent.class);
		DynamicAddedComponent dc = lookup(DynamicAddedComponent.class, "role");

		release(dc);

		dc = lookup(DynamicAddedComponent.class, "role");
		assertNotNull(dc);
	}

}
