package com.ctrip.hermes.core.schedule;

public interface SchedulePolicy {

	long fail(boolean shouldSleep);

	void succeess();

}
