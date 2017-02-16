package com.ctrip.hermes.consumer.pull;

public class OffsetRecord {

	private int m_partition;

	private boolean m_priority;

	private boolean m_resend;

	private long m_offset;

	private boolean m_nack;

	private int m_remainingRetries;

	private OffsetRecord(int partition, int priorityOrResend, boolean nack, int remainingRetries, long offset) {
		m_partition = partition;
		m_priority = priorityOrResend == 0;
		m_resend = priorityOrResend == Integer.MIN_VALUE;
		m_remainingRetries = remainingRetries;
		m_offset = offset;
		m_nack = nack;
	}

	public static OffsetRecord forNack(int partition, int priorityOrResend, int remainingRetries, long offset) {
		return new OffsetRecord(partition, priorityOrResend, true, remainingRetries, offset);
	}

	public static OffsetRecord forAck(int partition, int priorityOrResend, long offset) {
		return new OffsetRecord(partition, priorityOrResend, false, 0, offset);
	}

	public int getRemainingRetries() {
		return m_remainingRetries;
	}

	public int getPartition() {
		return m_partition;
	}

	public long getOffset() {
		return m_offset;
	}

	public boolean isPriority() {
		return m_priority;
	}

	public boolean isResend() {
		return m_resend;
	}

	public boolean isNack() {
		return m_nack;
	}

}
