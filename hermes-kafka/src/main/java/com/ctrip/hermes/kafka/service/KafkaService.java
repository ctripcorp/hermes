package com.ctrip.hermes.kafka.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.kafka.util.KafkaProperties;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Property;
import com.ctrip.hermes.meta.entity.Storage;

@Named
public class KafkaService {

	public enum RESET_POSITION {
		EARLIEST, LATEST
	};

	@Inject
	private MetaService m_metaService;

	private static final Logger m_logger = LoggerFactory.getLogger(KafkaService.class);

	public void resetConsumerOffset(String topic, String consumerGroup, RESET_POSITION position) {
		Properties prop = getProperties(topic, consumerGroup);
		KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(prop);
		List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
		List<TopicPartition> topicPartitions = new ArrayList<TopicPartition>();
		for (PartitionInfo partitionInfo : partitionInfos) {
			TopicPartition tp = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
			topicPartitions.add(tp);
		}
		consumer.assign(topicPartitions);
		Set<TopicPartition> assignment = consumer.assignment();
		List<PartitionInfo> partitions = consumer.partitionsFor(topic);
		if (assignment.size() != partitions.size()) {
			m_logger.warn("ASSIGNMENTS: " + assignment);
			m_logger.warn("PARTITIONS: " + partitions);
			m_logger.warn("Could not match, reset failed");
			consumer.close();
			return;
		}

		for (TopicPartition tp : assignment) {
			long before = consumer.position(tp);
			if (position == RESET_POSITION.EARLIEST) {
				consumer.seekToBeginning(tp);
			} else if (position == RESET_POSITION.LATEST) {
				consumer.seekToEnd(tp);
			}
			long after = consumer.position(tp);
			m_logger.info("Reset partition: {} From: {} To: {}", tp.partition(), before, after);
		}
		consumer.commitSync();
		consumer.close();
		m_logger.info("Reset offset Done");
	}

	private Properties getProperties(String topic, String consumerGroup) {
		Properties configs = new Properties();
		List<Partition> partitions = m_metaService.listPartitionsByTopic(topic);
		if (partitions == null || partitions.size() < 1) {
			return configs;
		}

		String consumerDatasource = partitions.get(0).getWriteDatasource();
		Storage targetStorage = m_metaService.findStorageByTopic(topic);
		if (targetStorage == null) {
			return configs;
		}

		for (Datasource datasource : targetStorage.getDatasources()) {
			if (consumerDatasource.equals(datasource.getId())) {
				Map<String, Property> properties = datasource.getProperties();
				for (Map.Entry<String, Property> prop : properties.entrySet()) {
					configs.put(prop.getValue().getName(), prop.getValue().getValue());
				}
				break;
			}
		}

		configs.put("group.id", consumerGroup);
		configs.put("enable.auto.commit", "false");
		return KafkaProperties.overrideByCtripDefaultConsumerSetting(configs, topic, consumerGroup, "");
	}
}
