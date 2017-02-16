package com.ctrip.hermes.consumer.api;


public class OffsetAndMetadata {

	private Long m_priorityOffset;

	private Long m_nonPriorityOffset;

	private Long m_resendOffset;

	public void setPriorityOffset(Long priorityOffset) {
		m_priorityOffset = priorityOffset;
	}

	public void setNonPriorityOffset(Long nonPriorityOffset) {
		m_nonPriorityOffset = nonPriorityOffset;
	}

	public void setResendOffset(Long resendOffset) {
		m_resendOffset = resendOffset;
	}

	public Long getPriorityOffset() {
		return m_priorityOffset;
	}

	public Long getNonPriorityOffset() {
		return m_nonPriorityOffset;
	}

	public Long getResendOffset() {
		return m_resendOffset;
	}

}
