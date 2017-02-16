package com.ctrip.hermes.core.bo;

import java.util.Date;

import org.unidal.tuple.Pair;

public class Offset {
	private long m_priorityOffset = 0L;

	private long m_nonPriorityOffset = 0L;

	private Pair<Date, Long> m_resendOffset;

	public Offset() {
		this(0L, 0L, null);
	}

	public Offset(long pOffset, long npOffset, Pair<Date, Long> rOffset) {
		m_priorityOffset = pOffset;
		m_nonPriorityOffset = npOffset;
		m_resendOffset = rOffset;
	}

	public long getPriorityOffset() {
		return m_priorityOffset;
	}

	public void setPriorityOffset(long priorityOffset) {
		m_priorityOffset = priorityOffset;
	}

	public long getNonPriorityOffset() {
		return m_nonPriorityOffset;
	}

	public void setNonPriorityOffset(long nonPriorityOffset) {
		m_nonPriorityOffset = nonPriorityOffset;
	}

	public Pair<Date, Long> getResendOffset() {
		return m_resendOffset;
	}

	public void setResendOffset(Pair<Date, Long> resendOffset) {
		m_resendOffset = resendOffset;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (m_nonPriorityOffset ^ (m_nonPriorityOffset >>> 32));
		result = prime * result + (int) (m_priorityOffset ^ (m_priorityOffset >>> 32));
		result = prime * result + ((m_resendOffset == null) ? 0 : m_resendOffset.hashCode());
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
		Offset other = (Offset) obj;
		if (m_nonPriorityOffset != other.m_nonPriorityOffset)
			return false;
		if (m_priorityOffset != other.m_priorityOffset)
			return false;
		if (m_resendOffset == null) {
			if (other.m_resendOffset != null)
				return false;
		} else if (!m_resendOffset.equals(other.m_resendOffset))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Offset [m_priorityOffset=" + m_priorityOffset + ", m_nonPriorityOffset=" + m_nonPriorityOffset
		      + ", m_resendOffset=" + m_resendOffset + "]";
	}

}
