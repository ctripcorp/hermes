package com.ctrip.hermes.core.bo;

public class AckContext {
	private long m_msgSeq;

	private int m_remainingRetries;

	private long m_onMessageStartTimeMillis;

	private long m_onMessageEndTimeMillis;

	public AckContext(long msgSeq, int remainingRetries, long onMessageStartTimeMillis, long onMessageEndTimeMillis) {
		m_msgSeq = msgSeq;
		m_remainingRetries = remainingRetries;
		m_onMessageStartTimeMillis = onMessageStartTimeMillis;
		m_onMessageEndTimeMillis = onMessageEndTimeMillis;
	}

	public long getMsgSeq() {
		return m_msgSeq;
	}

	public int getRemainingRetries() {
		return m_remainingRetries;
	}

	public void setOnMessageStartTimeMillis(long onMessageStartTimeMillis) {
		m_onMessageStartTimeMillis = onMessageStartTimeMillis;
	}

	public void setOnMessageEndTimeMillis(long onMessageEndTimeMillis) {
		m_onMessageEndTimeMillis = onMessageEndTimeMillis;
	}

	public long getOnMessageStartTimeMillis() {
		return m_onMessageStartTimeMillis;
	}

	public long getOnMessageEndTimeMillis() {
		return m_onMessageEndTimeMillis;
	}

	@Override
	public String toString() {
		return "AckContext [m_msgSeq=" + m_msgSeq + ", m_remainingRetries=" + m_remainingRetries
		      + ", m_onMessageStartTimeMillis=" + m_onMessageStartTimeMillis + ", m_onMessageEndTimeMillis="
		      + m_onMessageEndTimeMillis + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (m_msgSeq ^ (m_msgSeq >>> 32));
		result = prime * result + (int) (m_onMessageEndTimeMillis ^ (m_onMessageEndTimeMillis >>> 32));
		result = prime * result + (int) (m_onMessageStartTimeMillis ^ (m_onMessageStartTimeMillis >>> 32));
		result = prime * result + m_remainingRetries;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		AckContext other = (AckContext) obj;
		if (m_msgSeq != other.m_msgSeq)
			return false;
		if (m_onMessageEndTimeMillis != other.m_onMessageEndTimeMillis)
			return false;
		if (m_onMessageStartTimeMillis != other.m_onMessageStartTimeMillis)
			return false;
		if (m_remainingRetries != other.m_remainingRetries)
			return false;
		return true;
	}

}
