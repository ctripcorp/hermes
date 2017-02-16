package com.ctrip.hermes.consumer.api;

public class PullConsumerConfig {

	private int m_partitionMessageCacheSize = 1000;

	private int m_scanIntervalBase = 100;

	private int m_scanIntervalMax = 100;

	private int m_manualCommitInterval = 50;

	private int m_committerThreadCount = -1;

	public int getPartitionMessageCacheSize() {
		return m_partitionMessageCacheSize;
	}

	public int getScanIntervalBase() {
		return m_scanIntervalBase;
	}

	public int getScanIntervalMax() {
		return m_scanIntervalMax;
	}

	public int getManualCommitInterval() {
		return m_manualCommitInterval;
	}

	public void setManualCommitInterval(int manualCommitInterval) {
		m_manualCommitInterval = manualCommitInterval;
	}

	public void setPartitionMessageCacheSize(int partitionMessageCacheSize) {
		m_partitionMessageCacheSize = partitionMessageCacheSize;
	}

	public void setScanIntervalBase(int scanIntervalBase) {
		m_scanIntervalBase = scanIntervalBase;
	}

	public void setScanIntervalMax(int scanIntervalMax) {
		m_scanIntervalMax = scanIntervalMax;
	}

	public int getCommitterThreadCount() {
		return m_committerThreadCount;
	}

	public void setCommitterThreadCount(int committerThreadCount) {
		m_committerThreadCount = committerThreadCount;
	}

}
