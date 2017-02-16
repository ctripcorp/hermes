/**
 * 
 */
package com.ctrip.hermes.core.selector;

import static org.junit.Assert.assertArrayEquals;

import org.junit.Test;

/**
 * @author marsqing
 *
 *         Jun 29, 2016 1:51:14 PM
 */
public class CallbackContextTest {

	@Test
	public void testSingleMatch() {
		int slotCount = 3;
		SlotMatchResult[] slotMatchResults = new SlotMatchResult[slotCount];

		slotMatchResults[0] = new SlotMatchResult(0, true, 10, 15);
		slotMatchResults[1] = new SlotMatchResult(1, false, 20, -1);
		slotMatchResults[2] = new SlotMatchResult(2, false, 123456789, 123456788);

		CallbackContext ctx = new CallbackContext(slotMatchResults);

		Slot[] actualSlots = ctx.nextAwaitingSlots();

		Slot[] expSlots = new Slot[slotCount];
		expSlots[0] = new Slot(0, 16);
		expSlots[1] = new Slot(1, 20);
		expSlots[2] = new Slot(2, 123456789);

		assertArrayEquals(expSlots, actualSlots);
	}

	@Test
	public void testMultipleMatch() {
		int slotCount = 3;
		SlotMatchResult[] slotMatchResults = new SlotMatchResult[slotCount];

		slotMatchResults[0] = new SlotMatchResult(0, true, 10, 15);
		slotMatchResults[1] = new SlotMatchResult(1, false, 20, -1);
		slotMatchResults[2] = new SlotMatchResult(2, true, 123456789, 1234567890);

		CallbackContext ctx = new CallbackContext(slotMatchResults);

		Slot[] actualSlots = ctx.nextAwaitingSlots();

		Slot[] expSlots = new Slot[slotCount];
		expSlots[0] = new Slot(0, 16);
		expSlots[1] = new Slot(1, 20);
		expSlots[2] = new Slot(2, 1234567891);

		assertArrayEquals(expSlots, actualSlots);
	}

}
