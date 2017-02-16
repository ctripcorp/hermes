/**
 * 
 */
package com.ctrip.hermes.core.selector;

/**
 * @author marsqing
 *
 *         Jun 24, 2016 2:44:53 PM
 */
public class CallbackContext {

	private SlotMatchResult[] slotMatchResults;
	private long triggerTime;

	public CallbackContext(SlotMatchResult[] slotMatchResults) {
		this.slotMatchResults = slotMatchResults;
		this.triggerTime = System.currentTimeMillis();
	}

	public SlotMatchResult[] getSlotMatchResults() {
		return slotMatchResults;
	}

	public int getSlotCount() {
		return slotMatchResults.length;
	}

	public Slot[] nextAwaitingSlots() {
		Slot[] nextAwaitingSlots = new Slot[slotMatchResults.length];

		for (int i = 0; i < slotMatchResults.length; i++) {
			SlotMatchResult slotMatchResult = slotMatchResults[i];

			long nextAwaitingOffset;
			if (slotMatchResult.isMatch()) {
				nextAwaitingOffset = 1 + slotMatchResult.getTriggeringOffset();
			} else {
				nextAwaitingOffset = slotMatchResult.getAwaitingOffset();
			}

			nextAwaitingSlots[i] = new Slot(slotMatchResult.getIndex(), nextAwaitingOffset);
		}

		return nextAwaitingSlots;
	}

	public long getTriggerTime() {
		return triggerTime;
	}
	
}
