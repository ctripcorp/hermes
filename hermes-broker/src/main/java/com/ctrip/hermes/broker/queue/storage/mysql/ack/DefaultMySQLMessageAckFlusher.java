package com.ctrip.hermes.broker.queue.storage.mysql.ack;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.datasource.DataSource;
import org.unidal.dal.jdbc.datasource.DataSourceManager;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;
import org.unidal.tuple.Triple;

import com.ctrip.hermes.broker.dal.hermes.OffsetMessage;
import com.ctrip.hermes.broker.dal.hermes.OffsetResend;
import com.ctrip.hermes.core.constants.CatConstants;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.selector.CallbackContext;
import com.ctrip.hermes.core.selector.ExpireTimeHolder;
import com.ctrip.hermes.core.selector.FixedExpireTimeHolder;
import com.ctrip.hermes.core.selector.SelectorCallback;
import com.ctrip.hermes.core.selector.Slot;
import com.ctrip.hermes.core.selector.TriggerResult;
import com.ctrip.hermes.core.selector.TriggerResult.State;
import com.ctrip.hermes.env.config.broker.BrokerConfigProvider;
import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Topic;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Message;
import com.dianping.cat.message.Transaction;
import com.mchange.v2.resourcepool.TimeoutException;

@Named(type = MySQLMessageAckFlusher.class)
public class DefaultMySQLMessageAckFlusher implements MySQLMessageAckFlusher {
	private static final Logger log = LoggerFactory.getLogger(DefaultMySQLMessageAckFlusher.class);

	private static final ExpireTimeHolder NEVER_EXPIRE_TIME_HOLDER = new FixedExpireTimeHolder(Long.MAX_VALUE);

	private static final Comparator<OffsetMessage> OFFSET_MESSAGE_COMPARATOR = new Comparator<OffsetMessage>() {
		@Override
		public int compare(OffsetMessage o1, OffsetMessage o2) {
			return o1.getOffset() > o2.getOffset() ? 1 : o1.getOffset() == o2.getOffset() ? 0 : -1;
		}
	};

	private static final Comparator<OffsetResend> OFFSET_RESEND_COMPARATOR = new Comparator<OffsetResend>() {
		@Override
		public int compare(OffsetResend o1, OffsetResend o2) {
			return o1.getLastId() > o2.getLastId() ? 1 : o1.getLastId() == o2.getLastId() ? 0 : -1;
		}
	};

	@Inject
	private MetaService m_metaService;

	@Inject
	private DataSourceManager m_dataSourceManager;

	@Inject
	private BrokerConfigProvider m_config;

	@Inject
	private MySQLMessageAckSelectorManager m_ackSelectorManager;

	private ConcurrentMap<String, AckFlushHolder> m_ackFlushHolders = new ConcurrentHashMap<>();

	private class AckFlushHolder {
		private String m_topic;

		private AtomicLong m_ackedCount = new AtomicLong(0);

		private AtomicLong m_flushedCount = new AtomicLong(0);

		private ConcurrentMap<Triple<Integer, Boolean, Integer>, AckOffsetHolder<OffsetMessage>> m_messageOffsetHolders = new ConcurrentHashMap<>();

		private ConcurrentMap<Pair<Integer, Integer>, AckOffsetHolder<OffsetResend>> m_resendOffsetHolders = new ConcurrentHashMap<>();

		public AckFlushHolder(String topic) {
			m_topic = topic;
			m_ackSelectorManager.register(topic, NEVER_EXPIRE_TIME_HOLDER, new MessageAckSelectorCallback(topic), null, m_flushedCount.get());
		}

		public void addOffsetMessage(OffsetMessage offsetMessage) {
			Triple<Integer, Boolean, Integer> key = new Triple<>(offsetMessage.getPartition(), offsetMessage.getPriority() == 0, offsetMessage.getGroupId());
			getOrCreateAckOffsetHolder(m_messageOffsetHolders, key, OFFSET_MESSAGE_COMPARATOR).addAckOffset(offsetMessage);
			m_ackSelectorManager.getSelector().update(m_topic, true, new Slot(MySQLMessageAckSelectorManager.SLOT_NORMAL, m_ackedCount.incrementAndGet()));
		}

		public OffsetMessage getMaxAndResetOffsetMessageHolder(Triple<Integer, Boolean, Integer> key) {
			return getMaxOffsetAndResetOffsetHolder(key, m_messageOffsetHolders);
		}

		public void addOffsetResend(OffsetResend offsetResend) {
			Pair<Integer, Integer> key = new Pair<>(offsetResend.getPartition(), offsetResend.getGroupId());
			getOrCreateAckOffsetHolder(m_resendOffsetHolders, key, OFFSET_RESEND_COMPARATOR).addAckOffset(offsetResend);
			m_ackSelectorManager.getSelector().update(m_topic, true, new Slot(MySQLMessageAckSelectorManager.SLOT_NORMAL, m_ackedCount.incrementAndGet()));
		}

		public OffsetResend getMaxAndResetOffsetResendHolder(Pair<Integer, Integer> key) {
			return getMaxOffsetAndResetOffsetHolder(key, m_resendOffsetHolders);
		}

		public long getFlushedCount() {
			return m_flushedCount.get();
		}

		private <T, K> K getMaxOffsetAndResetOffsetHolder(T key, Map<T, AckOffsetHolder<K>> offsetHolders) {
			AckOffsetHolder<K> holder = offsetHolders.get(key);
			if (holder != null) {
				Pair<Integer, K> result = holder.getAndResetMaxAckOffsets();
				if (result.getKey() > 0) {
					m_flushedCount.addAndGet(result.getKey());
					return result.getValue();
				}
			}
			return null;
		}

		private <T, K> AckOffsetHolder<T> getOrCreateAckOffsetHolder(Map<K, AckOffsetHolder<T>> offsetHolders, K key, Comparator<T> comparator) {
			if (!offsetHolders.containsKey(key)) {
				synchronized (offsetHolders) {
					if (!offsetHolders.containsKey(key)) {
						offsetHolders.put(key, new AckOffsetHolder<T>(comparator));
					}
				}
			}
			return offsetHolders.get(key);
		}
	}

	private class AckOffsetHolder<T> {
		private BlockingQueue<T> m_ackOffsets = new LinkedBlockingQueue<>();

		private Comparator<T> m_comparator;

		public AckOffsetHolder(Comparator<T> comparator) {
			m_comparator = comparator;
		}

		public void addAckOffset(T ackOffset) {
			m_ackOffsets.offer(ackOffset);
		}

		public Pair<Integer, T> getAndResetMaxAckOffsets() {
			List<T> todos = new ArrayList<>();
			int retrievedCount = m_ackOffsets.drainTo(todos);
			T maxAckOffset = null;
			for (T t : todos) {
				if (maxAckOffset == null || m_comparator.compare(t, maxAckOffset) > 0) {
					maxAckOffset = t;
				}
			}
			return new Pair<>(retrievedCount, maxAckOffset);
		}
	}

	private AckFlushHolder getOrCreateAckFlushHolder(String topic) {
		AckFlushHolder holder = m_ackFlushHolders.get(topic);
		if (holder == null) {
			synchronized (m_ackFlushHolders) {
				holder = m_ackFlushHolders.get(topic);
				if (holder == null) {
					holder = new AckFlushHolder(topic);
					m_ackFlushHolders.put(topic, holder);

				}
			}
		}
		return holder;
	}

	private class MessageAckSelectorCallback implements SelectorCallback {
		private String m_topic;

		public MessageAckSelectorCallback(String topicName) {
			m_topic = topicName;
		}

		@Override
		public void onReady(CallbackContext ctx) {
			int totalExpectedAffectedOffsetCount = 0;
			int totalActualAffectedOffsetCount = 0;
			try {
				for (DatasourceAckFlushTask task : createAckFlushTasks(m_metaService.findTopicByName(m_topic))) {
					totalExpectedAffectedOffsetCount += task.getExpectedAffectedOffsetCount();
					Transaction tx = Cat.newTransaction(CatConstants.TYPE_ACK_FLUSH + task.getDatasourceName(), m_topic);
					try {
						int actualAffectedOffsetCount = task.execute();
						totalActualAffectedOffsetCount += actualAffectedOffsetCount;
						tx.addData("count", actualAffectedOffsetCount);
						tx.setStatus(Message.SUCCESS);
					} catch (Exception e) {
						tx.setStatus(e);
						log.error("Execute ack flush task failed. {}", task, e);
					} finally {
						tx.complete();
					}
				}
			} catch (Exception e) {
				log.error("Execute ack flush task failed.", e);
			} finally {
				State state = totalExpectedAffectedOffsetCount == 0 ? State.GotNothing
				      : totalExpectedAffectedOffsetCount == totalActualAffectedOffsetCount ? State.GotAndSuccessfullyProcessed : State.GotButErrorInProcessing;
				m_ackSelectorManager
				      .reRegister(m_topic, ctx, new TriggerResult(state, new long[] { m_ackFlushHolders.get(m_topic).getFlushedCount() }), NEVER_EXPIRE_TIME_HOLDER, this);
			}
		}

		private Collection<DatasourceAckFlushTask> createAckFlushTasks(Topic topic) {
			Map<String, DatasourceAckFlushTask> tasks = new HashMap<>();
			AckFlushHolder flushHolder = m_ackFlushHolders.get(topic.getName());
			if (flushHolder != null) {
				for (Partition partition : topic.getPartitions()) {
					DatasourceAckFlushTask dsTask = tasks.get(partition.getWriteDatasource());
					if (dsTask == null) {
						dsTask = new DatasourceAckFlushTask(partition.getReadDatasource(), topic.getId());
					}
					for (ConsumerGroup consumer : topic.getConsumerGroups()) {
						OffsetMessage priorityOffsetMessage = flushHolder.getMaxAndResetOffsetMessageHolder(new Triple<>(partition.getId(), true, consumer.getId()));
						if (priorityOffsetMessage != null) {
							dsTask.addOffsetMessage(priorityOffsetMessage);
						}

						OffsetMessage nonPriorityOffsetMessage = flushHolder.getMaxAndResetOffsetMessageHolder(new Triple<>(partition.getId(), false, consumer.getId()));
						if (nonPriorityOffsetMessage != null) {
							dsTask.addOffsetMessage(nonPriorityOffsetMessage);
						}

						Pair<Integer, Integer> offsetResendKey = new Pair<>(partition.getId(), consumer.getId());
						OffsetResend offsetResend = flushHolder.getMaxAndResetOffsetResendHolder(offsetResendKey);
						if (offsetResend != null) {
							dsTask.addOffsetResend(offsetResend);
						}

						if (!tasks.containsKey(partition.getWriteDatasource()) && dsTask.getExpectedAffectedOffsetCount() > 0) {
							tasks.put(partition.getWriteDatasource(), dsTask);
						}
					}
				}
			}
			return tasks.values();
		}
	}

	private class DatasourceAckFlushTask {
		private String m_datasourceName;

		private long m_topicId;

		private List<OffsetMessage> m_offsetMessages = new ArrayList<>();

		private List<OffsetResend> m_offsetResends = new ArrayList<>();

		public DatasourceAckFlushTask(String datasourceName, long topicId) {
			m_datasourceName = datasourceName;
			m_topicId = topicId;
		}

		public int execute() throws Exception {
			Connection conn = getConnection(m_datasourceName);
			int affectedCount = 0;
			if (conn != null) {
				Statement stmt = null;
				try {
					conn.setAutoCommit(false);
					stmt = conn.createStatement();
					for (OffsetMessage offsetMessage : m_offsetMessages) {
						stmt.addBatch(generateOffsetMessageUpdateSQL(offsetMessage));
					}
					for (OffsetResend offsetResend : m_offsetResends) {
						stmt.addBatch(generateOffsetResendUpdateSQL(offsetResend));
					}
					for (int sqlAffectedCount : stmt.executeBatch()) {
						affectedCount += sqlAffectedCount;
					}
					conn.commit();
				} catch (Exception e) {
					try {
						conn.rollback();
					} catch (Exception re) {
						log.error("Rollback failed.", re);
					}
					throw e;
				} finally {
					try {
						if (stmt != null) {
							stmt.close();
						}
					} catch (Exception e) {
						log.error("Close statement failed.", e);
					}
					try {
						conn.setAutoCommit(true);
					} catch (Exception e) {
						log.error("Close transaction failed.", e);
					}
					try {
						conn.close();
					} catch (Exception e) {
						log.error("Close connection failed.", e);
					}
				}
			}
			return affectedCount;
		}

		public String getDatasourceName() {
			return m_datasourceName;
		}

		private Connection getConnection(String datasourceName) throws Exception {
			try {
				DataSource ds = m_dataSourceManager.getDataSource(datasourceName);
				Connection conn = ds.getConnection();
				if (conn != null) {
					conn.setAutoCommit(true);
				}
				return conn;
			} catch (Exception e) {
				if (e instanceof SQLException && e.getCause() instanceof TimeoutException) {
					log.error("Checkout connection(for flushing ack to {}) timeout.", datasourceName);
				} else {
					log.error("Checkout connection(for flushing ack to {}) failed.", datasourceName);
				}
				throw e;
			}
		}

		private String generateOffsetMessageUpdateSQL(OffsetMessage offsetMessage) {
			// "UPDATE <TABLE/> SET <FIELDS/> WHERE <FIELD name='id'/> = ${id} AND <FIELD name='offset'/> < ${offset}"
			StringBuilder sb = new StringBuilder(256);
			sb.append("update " + m_topicId + "_" + offsetMessage.getPartition() + "_offset_message");
			sb.append(" set `offset`=" + offsetMessage.getOffset());
			sb.append(" where `id`=" + offsetMessage.getId());
			sb.append(" and `offset` < " + offsetMessage.getOffset());
			return sb.toString();
		}

		private String generateOffsetResendUpdateSQL(OffsetResend offsetResend) {
			// "UPDATE <TABLE/> SET <FIELDS/> WHERE <FIELD name='id'/> = ${id} AND <FIELD name='last-id'/> < ${last-id}"
			StringBuilder sb = new StringBuilder(256);
			sb.append("update " + m_topicId + "_" + offsetResend.getPartition() + "_offset_resend");
			sb.append(" set `last_id`=" + offsetResend.getLastId());
			sb.append(" where `id`=" + offsetResend.getId());
			sb.append(" and `last_id` < " + offsetResend.getLastId());
			return sb.toString();
		}

		public void addOffsetMessage(OffsetMessage offsetMessage) {
			m_offsetMessages.add(offsetMessage);
		}

		public void addOffsetResend(OffsetResend offsetResend) {
			m_offsetResends.add(offsetResend);
		}

		public int getExpectedAffectedOffsetCount() {
			return m_offsetMessages.size() + m_offsetResends.size();
		}

		@Override
		public String toString() {
			return "AckFlushTask [topic: " + m_topicId + ", datasource:" + m_datasourceName + ", offsetMessages=" + m_offsetMessages + ", offsetResends=" + m_offsetResends + "]";
		}
	}

	@Override
	public void ackOffsetMessage(OffsetMessage offsetMessage) {
		getOrCreateAckFlushHolder(offsetMessage.getTopic()).addOffsetMessage(cloneOffsetMessage(offsetMessage));
	}

	private OffsetMessage cloneOffsetMessage(OffsetMessage offsetMessage) {
		OffsetMessage om = new OffsetMessage();
		om.setId(offsetMessage.getId());
		om.setTopic(offsetMessage.getTopic());
		om.setPartition(offsetMessage.getPartition());
		om.setPriority(offsetMessage.getPriority());
		om.setGroupId(offsetMessage.getGroupId());
		om.setOffset(offsetMessage.getOffset());
		return om;
	}

	@Override
	public void ackOffsetResend(OffsetResend offsetResend) {
		getOrCreateAckFlushHolder(offsetResend.getTopic()).addOffsetResend(cloneOffsetResend(offsetResend));
	}

	private OffsetResend cloneOffsetResend(OffsetResend offsetResend) {
		OffsetResend or = new OffsetResend();
		or.setId(offsetResend.getId());
		or.setTopic(offsetResend.getTopic());
		or.setPartition(offsetResend.getPartition());
		or.setGroupId(offsetResend.getGroupId());
		or.setLastId(offsetResend.getLastId());
		return or;
	}
}
