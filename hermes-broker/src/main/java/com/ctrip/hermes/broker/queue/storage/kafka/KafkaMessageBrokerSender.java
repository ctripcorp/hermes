package com.ctrip.hermes.broker.queue.storage.kafka;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.net.Networks;

import com.ctrip.hermes.core.kafka.KafkaConstants;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Property;
import com.ctrip.hermes.meta.entity.Storage;

public class KafkaMessageBrokerSender {

	private static final Logger m_logger = LoggerFactory.getLogger(KafkaMessageBrokerSender.class);

	private KafkaProducer<String, byte[]> m_producer;

	private MetaService m_metaService;

	private String m_targetIdc;

	public KafkaMessageBrokerSender(String topic, MetaService metaService, String targetIdc) {
		this.m_metaService = metaService;
		this.m_targetIdc = targetIdc;
		Properties configs = getProducerProperties(topic);
		m_producer = new KafkaProducer<>(configs);
		m_logger.debug("Kafka broker sender for {} initialized.", topic);
	}

	private Properties getProducerProperties(String topic) {
		Properties configs = new Properties();

		List<Partition> partitions = m_metaService.listPartitionsByTopic(topic);
		if (partitions == null || partitions.size() < 1) {
			return configs;
		}

		String producerDatasource = partitions.get(0).getWriteDatasource();
		Storage producerStorage = m_metaService.findStorageByTopic(topic);
		if (producerStorage == null) {
			return configs;
		}

		String targetBootstrapServersPropertyName = String.format("%s.%s",
		      KafkaConstants.BOOTSTRAP_SERVERS_PROPERTY_NAME, m_targetIdc.toLowerCase());
		for (Datasource datasource : producerStorage.getDatasources()) {
			if (producerDatasource.equals(datasource.getId())) {
				Map<String, Property> properties = datasource.getProperties();
				for (Map.Entry<String, Property> prop : properties.entrySet()) {
					String propName = prop.getValue().getName();
					if (!propName.startsWith(KafkaConstants.BOOTSTRAP_SERVERS_PROPERTY_NAME)) {
						configs.put(propName, prop.getValue().getValue());
					}
				}

				if (properties.get(targetBootstrapServersPropertyName) != null) {
					configs.put(KafkaConstants.BOOTSTRAP_SERVERS_PROPERTY_NAME,
					      properties.get(targetBootstrapServersPropertyName).getValue());
				} else {
					configs.put(KafkaConstants.BOOTSTRAP_SERVERS_PROPERTY_NAME,
					      properties.get(KafkaConstants.BOOTSTRAP_SERVERS_PROPERTY_NAME).getValue());
				}
				
				break;
			}
		}

		return overrideByCtripDefaultSetting(configs, topic, m_targetIdc.toLowerCase());
	}

	/**
	 * 
	 * @param producerProp
	 * @return
	 */
	private Properties overrideByCtripDefaultSetting(Properties producerProp, String topic, String targetIdc) {
		producerProp.put("value.serializer", ByteArraySerializer.class.getCanonicalName());
		producerProp.put("key.serializer", StringSerializer.class.getCanonicalName());

		if (!producerProp.containsKey("client.id")) {
			producerProp.put("client.id", topic + "_" + Networks.forIp().getLocalHostAddress() + "_" + targetIdc);
		}
		if (!producerProp.containsKey("linger.ms")) {
			producerProp.put("linger.ms", 50);
		}
		if (!producerProp.containsKey("retries")) {
			producerProp.put("retries", 3);
		}

		return producerProp;
	}

	public void send(String topic, String partitionKey, byte[] array) {
		ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, partitionKey, array);
		m_producer.send(record);
	}

}
