package com.ctrip.hermes.kafka.producer;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.constants.IdcPolicy;
import com.ctrip.hermes.core.kafka.KafkaConstants;
import com.ctrip.hermes.core.kafka.KafkaIdcStrategy;
import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.message.codec.MessageCodec;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.env.ClientEnvironment;
import com.ctrip.hermes.kafka.util.KafkaProperties;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Property;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.producer.sender.MessageSender;

@Named(type = MessageSender.class, value = Endpoint.KAFKA)
public class KafkaMessageSender implements MessageSender, Initializable {

	private static final Logger m_logger = LoggerFactory.getLogger(KafkaMessageSender.class);

	private Map<String, Pair<KafkaProducer<String, byte[]>, Properties>> m_producers = new HashMap<String, Pair<KafkaProducer<String, byte[]>, Properties>>();;

	@Inject
	private MessageCodec m_codec;

	@Inject
	private MetaService m_metaService;

	@Inject
	private ClientEnvironment m_environment;

	@Inject
	private KafkaIdcStrategy m_kafkaIdcStrategy;

	private Properties getProducerProperties(String topic) {
		Properties configs = KafkaProperties.getDefaultKafkaProducerProperties();

		try {
			Properties envProperties = m_environment.getProducerConfig(topic);
			configs.putAll(envProperties);
		} catch (IOException e) {
			m_logger.warn("read producer config failed", e);
		}

		List<Partition> partitions = m_metaService.listPartitionsByTopic(topic);
		if (partitions == null || partitions.size() < 1) {
			return configs;
		}

		String producerDatasource = partitions.get(0).getWriteDatasource();
		Storage producerStorage = m_metaService.findStorageByTopic(topic);
		if (producerStorage == null) {
			return configs;
		}

		String targetIdc = getTargetIdc(topic).toLowerCase();
		String targetBootstrapServers = String.format("%s.%s", KafkaConstants.BOOTSTRAP_SERVERS_PROPERTY_NAME, targetIdc);

		for (Datasource datasource : producerStorage.getDatasources()) {
			if (producerDatasource.equals(datasource.getId())) {
				Map<String, Property> properties = datasource.getProperties();
				for (Map.Entry<String, Property> prop : properties.entrySet()) {
					String propName = prop.getValue().getName();
					if (!propName.startsWith(KafkaConstants.BOOTSTRAP_SERVERS_PROPERTY_NAME)) {
						configs.put(propName, prop.getValue().getValue());
					}
				}

				if (properties.get(targetBootstrapServers) != null) {
					configs.put(KafkaConstants.BOOTSTRAP_SERVERS_PROPERTY_NAME, properties.get(targetBootstrapServers)
					      .getValue());
				} else {
					configs.put(KafkaConstants.BOOTSTRAP_SERVERS_PROPERTY_NAME,
					      properties.get(KafkaConstants.BOOTSTRAP_SERVERS_PROPERTY_NAME).getValue());

				}

				break;
			}
		}

		return KafkaProperties.overrideByCtripDefaultProducerSetting(configs, topic, targetIdc);
	}

	private String getTargetIdc(String topic) {
		String idcPolicy = m_metaService.findTopicByName(topic).getIdcPolicy();
		if (idcPolicy == null) {
			idcPolicy = IdcPolicy.PRIMARY;
		}

		String targetIdc = m_kafkaIdcStrategy.getTargetIdc(idcPolicy);
		if (targetIdc == null) {
			throw new RuntimeException("Can not get target idc!");
		}

		return targetIdc;
	}

	/**
	 * 
	 * @param msg
	 * @return
	 */
	@Override
	public Future<SendResult> send(ProducerMessage<?> msg) {
		String topic = msg.getTopic();
		String partition = msg.getPartitionKey();

		if (!m_producers.containsKey(topic)) {
			synchronized (m_producers) {
				if (!m_producers.containsKey(topic)) {
					Properties configs = getProducerProperties(topic);
					long start = System.currentTimeMillis();
					KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(configs);
					long end = System.currentTimeMillis();
					m_logger.info("Init kafka producer in " + (end - start) + "ms");
					m_producers.put(topic, new Pair<KafkaProducer<String, byte[]>, Properties>(producer, configs));
				}
			}
		}

		KafkaProducer<String, byte[]> producer = m_producers.get(topic).getKey();

		byte[] bytes = m_codec.encode(msg);

		ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>(topic, partition, bytes);

		Future<RecordMetadata> sendResult = null;
		if (msg.getCallback() != null) {
			sendResult = producer.send(record, new KafkaCallback(msg.getCallback()));
		} else {
			sendResult = producer.send(record);
		}

		return new KafkaFuture(sendResult);
	}

	// FIXME how to close when singleton
	public void close() {
		for (Pair<KafkaProducer<String, byte[]>, Properties> producerAndProperties : m_producers.values()) {
			producerAndProperties.getKey().close();
		}
		m_producers.clear();
	}

	@Override
	public void initialize() throws InitializationException {
		Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create("CheckIdcPolicyChangesExecutor", true))
		      .scheduleWithFixedDelay(new Runnable() {
			      @Override
			      public void run() {
				      try {
					      for (Entry<String, Pair<KafkaProducer<String, byte[]>, Properties>> producer : m_producers.entrySet()) {
						      Properties newProps = getProducerProperties(producer.getKey());
						      Properties currentProps = producer.getValue().getValue();

						      String newBootstrapServersProperty = newProps
						            .getProperty(KafkaConstants.BOOTSTRAP_SERVERS_PROPERTY_NAME);
						      String currentBootstrapServersProperty = currentProps
						            .getProperty(KafkaConstants.BOOTSTRAP_SERVERS_PROPERTY_NAME);

						      if (!(newBootstrapServersProperty != null && currentBootstrapServersProperty != null && newBootstrapServersProperty
						            .equals(currentBootstrapServersProperty))) {
							      synchronized (m_producers) {
								      if (m_producers.containsKey(producer.getKey())) {
									      m_logger.info("Sending messages to topic:{} on new kafka cluster:{} instead of :{}.",
									            producer.getKey(), newBootstrapServersProperty, currentBootstrapServersProperty);
									      Pair<KafkaProducer<String, byte[]>, Properties> removedProducer = m_producers
									            .remove(producer.getKey());
									      removedProducer.getKey().close();
								      }
							      }

						      }

					      }
				      } catch (Exception e) {
					      m_logger.warn("Check idc policy changes failed!", e);
				      }
			      }
		      }, 5, 20, TimeUnit.SECONDS);
	}
}
