package com.ctrip.hermes.consumer.pull;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.consumer.api.OffsetAndMetadata;
import com.ctrip.hermes.consumer.api.OffsetCommitCallback;
import com.ctrip.hermes.consumer.api.TopicPartition;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.google.common.util.concurrent.SettableFuture;

public class RetriveSnapshot<T> {
	// Partition => OffsetRecord
	private ConcurrentMap<Integer, List<OffsetRecord>> m_offsetRecords;

	private SettableFuture<Boolean> m_future;

	private ConcurrentMap<Integer, Boolean> m_pendingPartitions = new ConcurrentHashMap<>();

	private String m_topic;

	private Committer<T> m_committer;

	private AtomicBoolean m_done = new AtomicBoolean(false);

	public RetriveSnapshot(String topic, List<ConsumerMessage<T>> msgs, Committer<T> committer) {
		m_topic = topic;
		m_offsetRecords = new ConcurrentHashMap<>();
		m_committer = committer;

		for (ConsumerMessage<T> msg : msgs) {
			int priorityOrResend = unifyPriorityAndResend(msg);
			int partition = msg.getPartition();
			long offset = msg.getOffset();
			Pair<Integer, Integer> ppr = new Pair<>(partition, priorityOrResend);
			HashMap<Pair<Integer, Integer>, Long> offsets = new HashMap<>();

			// find max offset for each "table"
			Long lastOffset = offsets.get(ppr);
			if (lastOffset == null || lastOffset < offset) {
				offsets.put(ppr, offset);
			}

			for (Entry<Pair<Integer, Integer>, Long> entry : offsets.entrySet()) {
				addAckRecord(entry.getKey().getKey(), entry.getKey().getValue(), entry.getValue());
			}

			((PullBrokerConsumerMessage<T>) msg).setOwningSnapshot(this);
		}
	}

	public synchronized void addNackMessage(ConsumerMessage<T> msg) {
		if (!m_done.get()) {
			// can't nack after commitAsync, otherwise the added partition may keep the future from complete
			addNackRecord(msg.getPartition(), unifyPriorityAndResend(msg), msg.getRemainingRetries(), msg.getOffset());
		}
	}

	public synchronized SettableFuture<Boolean> commitAsync(final OffsetCommitCallback callback,
	      ExecutorService callbackExecutor) {
		m_done.set(true);
		SettableFuture<Boolean> future = SettableFuture.create();

		if (callback != null) {
			future.addListener(new Runnable() {

				@Override
				public void run() {
					Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
					for (Map.Entry<Integer, List<OffsetRecord>> entry : getOffsetRecords().entrySet()) {
						TopicPartition tp = new TopicPartition(m_topic, entry.getKey());
						List<OffsetRecord> records = entry.getValue();

						if (records != null && !records.isEmpty()) {
							OffsetAndMetadata offsetMeta = offsets.get(tp);
							if (offsetMeta == null) {
								offsetMeta = new OffsetAndMetadata();
								offsets.put(tp, offsetMeta);
							}

							for (OffsetRecord rec : entry.getValue()) {
								if (rec.isNack()) {
									// TODO add nack info to OffsetAndMetadata
								} else {
									if (rec.isResend()) {
										offsetMeta.setResendOffset(rec.getOffset());
									} else {
										if (rec.isPriority()) {
											offsetMeta.setPriorityOffset(rec.getOffset());
										} else {
											offsetMeta.setNonPriorityOffset(rec.getOffset());
										}
									}
								}

							}
						}
					}

					callback.onComplete(offsets, null);
				}

			}, callbackExecutor);
		}

		m_future = future;

		m_committer.scanAndCommitAsync();

		return future;
	}

	public boolean isDone() {
		return m_done.get();
	}

	private void addAckRecord(int partition, int priorityOrResend, long offset) {
		m_pendingPartitions.put(partition, Boolean.FALSE);
		createOrFindRecords(partition).add(OffsetRecord.forAck(partition, priorityOrResend, offset));
	}

	private void addNackRecord(int partition, int priorityOrResend, int remainingRetries, long offset) {
		m_pendingPartitions.put(partition, Boolean.FALSE);
		createOrFindRecords(partition).add(OffsetRecord.forNack(partition, priorityOrResend, remainingRetries, offset));
	}

	private int unifyPriorityAndResend(ConsumerMessage<T> msg) {
		if (msg.isResend()) {
			return Integer.MIN_VALUE;
		} else {
			return msg.isPriority() ? 0 : 1;
		}
	}

	private List<OffsetRecord> createOrFindRecords(int partition) {
		List<OffsetRecord> records = m_offsetRecords.get(partition);

		if (records == null) {
			m_offsetRecords.putIfAbsent(partition, new LinkedList<OffsetRecord>());
			records = m_offsetRecords.get(partition);
		}

		return records;
	}

	public ConcurrentMap<Integer, List<OffsetRecord>> getOffsetRecords() {
		return m_offsetRecords;
	}

	public void notifyFuture() {
		if (m_future != null) {
			m_future.set(true);
		}
	}

	public synchronized void partitionDone(int partition) {
		m_pendingPartitions.remove(partition);
		if (m_pendingPartitions.isEmpty()) {
			if (m_future != null) {
				m_future.set(true);
			}
		}
	}

}
