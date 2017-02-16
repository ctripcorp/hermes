package com.ctrip.hermes.broker.queue.storage.mysql.dal;

import java.util.Map;

import org.unidal.dal.jdbc.QueryDef;
import org.unidal.dal.jdbc.QueryEngine;
import org.unidal.dal.jdbc.QueryType;
import org.unidal.dal.jdbc.mapping.TableProvider;
import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.meta.entity.Partition;

public class HermesTableProvider implements TableProvider {

	private static final String DEAD_LETTER_TABLE = "dead-letter";

	private static final String RESEND_OFFSET_TABLE = "offset-resend";

	private static final String MESSAGE_OFFSET_TABLE = "offset-message";

	private static final String RESEND_MESSAGE_TABLE = "resend-group-id";

	private static final String MESSAGE_TABLE = "message-priority";

	@Inject
	private MetaService m_metaService;

	private String m_tablePrefix = "";

	@Override
	public String getDataSourceName(Map<String, Object> hints, String logicalTableName) {
		QueryDef def = (QueryDef) hints.get(QueryEngine.HINT_QUERY);
		TopicPartitionAware tpAware = (TopicPartitionAware) hints.get(QueryEngine.HINT_DATA_OBJECT);

		return findDataSourceName(def, tpAware, logicalTableName);
	}

	@Override
	public String getPhysicalTableName(Map<String, Object> hints, String logicalTableName) {
		TopicPartitionAware tpAware = (TopicPartitionAware) hints.get(QueryEngine.HINT_DATA_OBJECT);

		String fmt = null;
		String db = toDbName(tpAware.getTopic(), tpAware.getPartition());
		switch (logicalTableName) {
		case MESSAGE_TABLE:
			fmt = m_tablePrefix + "%s_message_%s";
			TopicPartitionPriorityAware tppAware = (TopicPartitionPriorityAware) tpAware;
			return String.format(fmt, db, tppAware.getPriority());

		case RESEND_MESSAGE_TABLE:
			fmt = m_tablePrefix + "%s_resend_%s";
			TopicPartitionPriorityGroupAware tpgAware = (TopicPartitionPriorityGroupAware) tpAware;
			return String.format(fmt, db, tpgAware.getGroupId());

		case MESSAGE_OFFSET_TABLE:
			fmt = m_tablePrefix + "%s_offset_message";
			return String.format(fmt, db);

		case RESEND_OFFSET_TABLE:
			fmt = m_tablePrefix + "%s_offset_resend";
			return String.format(fmt, db);

		case DEAD_LETTER_TABLE:
			fmt = m_tablePrefix + "%s_dead_letter";
			return String.format(fmt, db);

		default:
			throw new IllegalArgumentException(String.format("Unknown physical table for %s %s", hints, logicalTableName));
		}

	}

	private long findTopicId(String topic) {
		return m_metaService.findTopicByName(topic).getId();
	}

	private String toDbName(String topic, int partition) {
		String fmt = "%s_%s";
		return String.format(fmt, findTopicId(topic), partition);
	}

	private String findDataSourceName(QueryDef def, TopicPartitionAware tpAware, String logicalTableName) {
		QueryType queryType = def.getType();

		Partition p = m_metaService.findPartitionByTopicAndPartition(tpAware.getTopic(), tpAware.getPartition());

		switch (queryType) {
		case INSERT:
			return MESSAGE_TABLE.equals(logicalTableName) ? p.getWriteDatasource() : p.getReadDatasource();
		case SELECT:
		case UPDATE:
		case DELETE:
			return p.getReadDatasource();

		default:
			throw new IllegalArgumentException(String.format("Unknown query type '%s'", queryType));
		}
	}

	public void setTablePrefix(String tablePrefix) {
		m_tablePrefix = tablePrefix;
	}

}
