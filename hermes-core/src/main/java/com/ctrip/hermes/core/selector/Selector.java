package com.ctrip.hermes.core.selector;

public interface Selector<T> {

	public enum InitialLastUpdateTime {
		OLDEST, NEWEST
	}

	void register(T key, ExpireTimeHolder expireTimeHolder, SelectorCallback callback, long lastTriggerTime, Slot... slots);
	
	void update(T key, boolean refreshUpdateTimeWhenNoOb, Slot... slots);

	void updateAll(boolean refreshUpdateTimeWhenNoOb, Slot... slots);
	
	int getSlotCount();

}
