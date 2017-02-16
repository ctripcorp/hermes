package com.ctrip.hermes.consumer.pull;

import java.util.LinkedList;
import java.util.List;

public class PartitionOperation<T> {

	private int m_partition;

	private List<OffsetRecord> m_records;

	private List<RetriveSnapshot<T>> m_retriveSnapshots = new LinkedList<>();

	public PartitionOperation(int partition, List<OffsetRecord> records, RetriveSnapshot<T> retriveSnapshot) {
		m_partition = partition;
		m_records = records;
		m_retriveSnapshots.add(retriveSnapshot);
	}

	public List<OffsetRecord> getRecords() {
		return m_records;
	}

	public void merge(PartitionOperation<T> op) {
		m_records.addAll(op.m_records);
		m_retriveSnapshots.addAll(op.m_retriveSnapshots);
	}

	public int getPartition() {
		return m_partition;
	}

	public void done() {
		for (RetriveSnapshot<T> snapshot : m_retriveSnapshots) {
			snapshot.partitionDone(m_partition);
		}
	}

}
