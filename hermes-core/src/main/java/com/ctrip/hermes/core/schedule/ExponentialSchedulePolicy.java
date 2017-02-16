package com.ctrip.hermes.core.schedule;

public class ExponentialSchedulePolicy implements SchedulePolicy {

	private int m_lastDelayTime;

	private final int m_delayBase;

	private final int m_delayUpperbound;

	public ExponentialSchedulePolicy(int delayBase, int delayUpperbound) {
		m_delayBase = delayBase;
		m_delayUpperbound = delayUpperbound;
	}

	@Override
	public long fail(boolean shouldSleep) {
		int delayTime = m_lastDelayTime;

		if (delayTime == 0) {
			delayTime = m_delayBase;
		} else {
			delayTime = Math.min(m_lastDelayTime << 1, m_delayUpperbound);
		}

		if (shouldSleep) {
			try {
				Thread.sleep(delayTime);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}

		m_lastDelayTime = delayTime;
		return delayTime;
	}

	@Override
	public void succeess() {
		m_lastDelayTime = 0;
	}

}
