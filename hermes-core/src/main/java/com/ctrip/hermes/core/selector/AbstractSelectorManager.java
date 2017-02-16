/**
 * 
 */
package com.ctrip.hermes.core.selector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * @author marsqing
 *
 *         Jul 7, 2016 12:03:44 PM
 */
public abstract class AbstractSelectorManager<T> implements SelectorManager<T> {

	private static Logger log = LoggerFactory.getLogger(AbstractSelectorManager.class);

	protected abstract long nextAwaitingNormalOffset(T key, long doneOffset, Object arg);

	protected abstract long nextAwaitingSafeTriggerOffset(T key, Object arg);

	@Override
	public void reRegister(T key, CallbackContext callbackCtx, TriggerResult triggerResult, ExpireTimeHolder expireTimeHolder, SelectorCallback callback) {
		SlotMatchResult[] slotMatchResults = callbackCtx.getSlotMatchResults();
		int slotCount = slotMatchResults.length;
		long[] doneOffsets = triggerResult.getDoneOffsets();

		long nextAwaitingOffset;
		Slot[] nextAwaitingSlots = null;

		switch (triggerResult.getState()) {
		case GotAndSuccessfullyProcessed:
			nextAwaitingSlots = new Slot[slotCount];

			for (int i = 0; i < slotCount - 1; i++) {
				if (doneOffsets[i] > 0) {
					nextAwaitingOffset = nextAwaitingNormalOffset(key, doneOffsets[i], triggerResult.getArg());
				} else {
					if (slotMatchResults[i].isMatch()) {
						/**
						 * slot match, but got nothing from this slot, should
						 * not happen, log and bump awaiting offset to avoid
						 * infinite triggering
						 */
						log.warn("Slot match, but nothing got from this slot");
						nextAwaitingOffset = nextAwaitingNormalOffset(key, slotMatchResults[i].biggerOffset(), triggerResult.getArg());
						// biggerOffset is triggeringOffset
					} else {
						nextAwaitingOffset = slotMatchResults[i].getAwaitingOffset();
					}
				}
				nextAwaitingSlots[i] = new Slot(i, nextAwaitingOffset);
			}

			nextAwaitingSlots[slotCount - 1] = new Slot(slotCount - 1, nextAwaitingSafeTriggerOffset(key, triggerResult.getArg()));
			break;

		case GotNothing:
			/**
			 * Triggered but got nothing, usually triggered by safe trigger.
			 * Bump all matched awaiting offset to avoid immediately triggered
			 * again.
			 */
			nextAwaitingSlots = new Slot[slotCount];

			for (int i = 0; i < slotCount - 1; i++) {
				if (slotMatchResults[i].isMatch()) {
					nextAwaitingOffset = nextAwaitingNormalOffset(key, slotMatchResults[i].biggerOffset(), triggerResult.getArg());
					// biggerOffset is triggeringOffset
				} else {
					nextAwaitingOffset = slotMatchResults[i].getAwaitingOffset();
				}
				nextAwaitingSlots[i] = new Slot(i, nextAwaitingOffset);
			}

			nextAwaitingSlots[slotCount - 1] = new Slot(slotCount - 1, nextAwaitingSafeTriggerOffset(key, triggerResult.getArg()));
			break;

		case GotButErrorInProcessing:
			/**
			 * Some elements is got, but some error occurred when processing,
			 * and the elements are returned. So wait until triggered by safe
			 * trigger.
			 */
			nextAwaitingSlots = new Slot[1];

			nextAwaitingSlots[0] = new Slot(slotCount - 1, nextAwaitingSafeTriggerOffset(key, triggerResult.getArg()));
			break;
		}

		getSelector().register(key, expireTimeHolder, callback, callbackCtx.getTriggerTime(), nextAwaitingSlots);
	}

	@Override
	public void register(T key, ExpireTimeHolder expireTimeHolder, SelectorCallback callback, Object arg, long... initDoneOffsets) {
		int slotCount = getSelector().getSlotCount();
		Preconditions.checkArgument(initDoneOffsets.length == slotCount - 1, //
				"initDoneOffsets length %s is not slot count %s minus 1", initDoneOffsets.length, slotCount);

		Slot[] slots = new Slot[slotCount];

		for (int i = 0; i < slotCount - 1; i++) {
			slots[i] = new Slot(i, nextAwaitingNormalOffset(key, initDoneOffsets[i], arg));
		}

		slots[slotCount - 1] = new Slot(slotCount - 1, nextAwaitingSafeTriggerOffset(key, arg));

		getSelector().register(key, expireTimeHolder, callback, System.currentTimeMillis(), slots);
	}

}
