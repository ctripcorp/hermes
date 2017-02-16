package com.ctrip.hermes.consumer.pull;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.consumer.api.PullConsumerConfig;
import com.ctrip.hermes.consumer.engine.ack.AckManager;
import com.ctrip.hermes.consumer.engine.config.ConsumerConfig;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.transport.command.v5.AckMessageCommandV5;
import com.ctrip.hermes.core.utils.HermesThreadFactory;

public class DefaultCommitter<T> implements Committer<T> {

	private final static Logger log = LoggerFactory.getLogger(DefaultCommitter.class);

	private String m_topic;

	private String m_groupId;

	private PullConsumerConfig m_config;

	private AtomicBoolean m_stopped = new AtomicBoolean(false);

	private BlockingQueue<RetriveSnapshot<T>> m_snapshots = new LinkedBlockingQueue<>();

	private ConcurrentMap<Integer, PartitionOperationHolder> m_partitionOperationHolders = new ConcurrentHashMap<>();

	private AckManager m_ackManager;

	private ConsumerConfig m_consumerConfig;

	private ExecutorService m_committerIoThreadPool;

	public DefaultCommitter(String topic, String groupId, int partitionCount, PullConsumerConfig config,
	      AckManager ackManager, ConsumerConfig consumerConfig) {
		m_topic = topic;
		m_groupId = groupId;
		m_config = config;
		m_ackManager = ackManager;
		m_consumerConfig = consumerConfig;

		int committerThreadCount = m_config.getCommitterThreadCount() <= 0 ? Math.min(10, partitionCount) : m_config
		      .getCommitterThreadCount();
		startCommitter(topic, groupId, committerThreadCount);
	}

	private void startCommitter(String topic, String groupId, int threadCount) {
		m_committerIoThreadPool = Executors.newFixedThreadPool(threadCount,
		      HermesThreadFactory.create("PullConsumerManualOffsetCommitIO-" + topic + "-" + groupId, true));

		ManualCommitChecker manualCommitChecker = new ManualCommitChecker();
		ThreadFactory factory = HermesThreadFactory.create("PullConsumerManualOffsetCommitChecker-" + topic + "-"
		      + groupId, true);
		factory.newThread(manualCommitChecker).start();
	}

	private boolean writeAckToBroker(int partition, List<OffsetRecord> partitionRecords) {
		boolean success = true;

		if (partitionRecords != null && !partitionRecords.isEmpty()) {
			AckMessageCommandV5 cmd = new AckMessageCommandV5(m_topic, partition, m_groupId,
			      m_consumerConfig.getAckCheckerResultTimeoutMillis());

			for (OffsetRecord rec : partitionRecords) {
				if (rec.isNack()) {
					cmd.addNackMsg(rec.isPriority(), rec.isResend(), rec.getOffset(), rec.getRemainingRetries(), 0, 0);
				} else {
					cmd.addAckMsg(rec.isPriority(), rec.isResend(), rec.getOffset(), 0, 0, 0);
				}
			}
			success = m_ackManager.writeAckToBroker(cmd);
		}

		return success;
	}

	private class PartitionOperationHolder {
		private BlockingDeque<PartitionOperation<T>> m_operations = new LinkedBlockingDeque<PartitionOperation<T>>();

		private AtomicBoolean m_flushing = new AtomicBoolean(false);

		private int m_partition;

		public PartitionOperationHolder(int partition) {
			m_partition = partition;
		}

		public void submit(PartitionOperation<T> operation) {
			m_operations.offer(operation);
		}

		public void flush(ExecutorService threadPool) {
			threadPool.submit(new Runnable() {

				@Override
				public void run() {
					PartitionOperation<T> op = null;
					try {
						op = m_operations.poll();
						if (op != null) {
							op = mergeMoreOperation(op, m_operations, 5000);

							if (writeAckToBroker(m_partition, op.getRecords())) {
								op.done();
								op = null;
							}
						}
					} catch (Exception e) {
						log.warn(
						      "Unexpected exception when commit consumer offet to broker, topic: {}, groupId: {}, partition: {}",
						      m_topic, m_groupId, m_partition, e);
					} finally {
						try {
							if (op != null) {
								m_operations.addFirst(op);
							}
						} catch (Exception e) {
							log.warn(
							      "Unexpected exception when push back operation to PartitionHolder, topic: {}, groupId: {}, partition: {}",
							      m_topic, m_groupId, m_partition, e);
						} finally {
							m_flushing.set(false);
						}
					}
				}
			});
		}

		public boolean startFlushing() {
			return m_flushing.compareAndSet(false, true);
		}

	}

	private class ManualCommitChecker implements Runnable {
		@Override
		public void run() {

			while (!m_stopped.get()) {
				List<Entry<Integer, PartitionOperationHolder>> entryList = new LinkedList<>(
				      m_partitionOperationHolders.entrySet());
				Collections.shuffle(entryList);

				for (Entry<Integer, PartitionOperationHolder> entry : entryList) {
					final PartitionOperationHolder holder = entry.getValue();

					if (!holder.startFlushing()) {
						holder.flush(m_committerIoThreadPool);
					}
				}

				try {
					TimeUnit.MILLISECONDS.sleep(m_config.getManualCommitInterval());
				} catch (InterruptedException e) {
					// ignore
				}
			}
		}
	}

	private PartitionOperation<T> mergeMoreOperation(PartitionOperation<T> op,
	      BlockingDeque<PartitionOperation<T>> opQueue, int maxRecords) {

		while (!opQueue.isEmpty() && op.getRecords().size() < maxRecords) {
			if (op.getRecords().size() + opQueue.peek().getRecords().size() <= maxRecords) {
				PartitionOperation<T> moreOp = opQueue.poll();
				op.merge(moreOp);
			} else {
				break;
			}
		}

		return op;
	}

	private void commitSnapshotAsync(RetriveSnapshot<T> snapshot) {
		if (snapshot.getOffsetRecords().isEmpty()) {
			// ensure snapshot's Future is called
			snapshot.notifyFuture();
		} else {
			for (Entry<Integer, List<OffsetRecord>> entry : snapshot.getOffsetRecords().entrySet()) {
				Integer partition = entry.getKey();

				PartitionOperationHolder holder = m_partitionOperationHolders.get(partition);
				if (holder == null) {
					m_partitionOperationHolders.putIfAbsent(partition, new PartitionOperationHolder(partition));
					holder = m_partitionOperationHolders.get(partition);
				}

				holder.submit(new PartitionOperation<T>(partition, entry.getValue(), snapshot));
			}
		}
	}

	@Override
	public void close() {
		m_stopped.compareAndSet(false, true);
		m_committerIoThreadPool.shutdown();
	}

	@Override
	public synchronized RetriveSnapshot<T> delivered(List<ConsumerMessage<T>> msgs) {
		RetriveSnapshot<T> snapshot = new RetriveSnapshot<T>(m_topic, msgs, this);
		m_snapshots.add(snapshot);
		return snapshot;
	}

	@Override
	public synchronized void scanAndCommitAsync() {
		while (!m_snapshots.isEmpty()) {
			RetriveSnapshot<T> snapshot = m_snapshots.peek();
			if (snapshot.isDone()) {
				m_snapshots.poll();
				commitSnapshotAsync(snapshot);
			} else {
				break;
			}
		}
	}

}
