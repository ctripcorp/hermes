package com.ctrip.hermes.kafka.engine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.consumer.engine.ConsumerContext;
import com.ctrip.hermes.consumer.engine.SubscribeHandle;
import com.ctrip.hermes.consumer.engine.bootstrap.BaseConsumerBootstrap;
import com.ctrip.hermes.consumer.engine.bootstrap.ConsumerBootstrap;
import com.ctrip.hermes.consumer.engine.config.ConsumerConfig;
import com.ctrip.hermes.core.constants.IdcPolicy;
import com.ctrip.hermes.core.kafka.KafkaConstants;
import com.ctrip.hermes.core.kafka.KafkaIdcStrategy;
import com.ctrip.hermes.core.message.BaseConsumerMessage;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.message.codec.MessageCodec;
import com.ctrip.hermes.core.schedule.ExponentialSchedulePolicy;
import com.ctrip.hermes.core.schedule.SchedulePolicy;
import com.ctrip.hermes.core.transport.command.CorrelationIdGenerator;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.env.ClientEnvironment;
import com.ctrip.hermes.kafka.message.KafkaConsumerMessage;
import com.ctrip.hermes.kafka.util.KafkaProperties;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Property;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

@Named(type = ConsumerBootstrap.class, value = Endpoint.KAFKA)
public class KafkaConsumerBootstrap extends BaseConsumerBootstrap implements Initializable {

	private static final Logger m_logger = LoggerFactory.getLogger(KafkaConsumerBootstrap.class);

	private ExecutorService m_executor = Executors.newCachedThreadPool(HermesThreadFactory.create(
	      "KafkaConsumerExecutor", true));

	@Inject
	private ClientEnvironment m_environment;

	@Inject
	private MessageCodec m_messageCodec;

	@Inject
	private ConsumerConfig m_consumerConfig;

	@Inject
	private KafkaIdcStrategy m_kafkaIdcStrategy;

	private Map<ConsumerContext, KafkaConsumerThread> consumers = new HashMap<ConsumerContext, KafkaConsumerThread>();

	private Map<ConsumerContext, Long> tokens = new HashMap<ConsumerContext, Long>();

	@Override
	protected SubscribeHandle doStart(final ConsumerContext consumerContext) {

		Topic topic = consumerContext.getTopic();

		Properties props = getConsumerProperties(topic.getName(), consumerContext.getGroupId());

		KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(props);

		final long token = CorrelationIdGenerator.generateCorrelationId();
		m_consumerNotifier.register(token, consumerContext);
		KafkaConsumerThread consumerThread = new KafkaConsumerThread(consumer, consumerContext, props, token);
		m_executor.submit(consumerThread);

		consumers.put(consumerContext, consumerThread);
		tokens.put(consumerContext, token);

		return new SubscribeHandle() {

			@Override
			public void close() {
				m_logger.info("Stopping kafka consumer with token: " + token);
				if (consumers.containsKey(consumerContext)) {
					synchronized (consumers) {
						if (consumers.containsKey(consumerContext)) {
							doStop(consumerContext);
						}
					}
				}
			}
		};
	}

	@Override
	protected void doStop(ConsumerContext consumerContext) {
		KafkaConsumerThread consumerThread = consumers.remove(consumerContext);
		if (consumerThread != null)
			consumerThread.shutdown();

		Long token = tokens.remove(consumerContext);
		if (token != null)
			m_consumerNotifier.deregister(token, true);

		super.doStop(consumerContext);
	}

	private void restart(ConsumerContext consumerContext) {
		doStop(consumerContext);
		doStart(consumerContext);
	}

	class KafkaConsumerThread implements Runnable {

		private final AtomicBoolean closed = new AtomicBoolean(false);

		private KafkaConsumer<String, byte[]> consumer;

		private ConsumerContext consumerContext;

		private Properties props;

		private long token;

		public KafkaConsumerThread(KafkaConsumer<String, byte[]> consumer, ConsumerContext consumerContext,
		      Properties props, long token) {
			this.consumer = consumer;
			this.consumerContext = consumerContext;
			this.props = props;
			this.token = token;
		}

		@Override
		public void run() {
			try {
				consumer.subscribe(Arrays.asList(consumerContext.getTopic().getName()));
				Set<TopicPartition> assignment = consumer.assignment();
				m_logger.info("Current assignment: " + assignment);
				m_logger.info("Starting kafka consumer with token: " + token);
				SchedulePolicy retryPolicy = new ExponentialSchedulePolicy(m_consumerConfig.getKafkaPollFailWaitBase(),
				      m_consumerConfig.getKafkaPollFailWaitMax());
				while (!closed.get()) {
					ConsumerRecords<String, byte[]> records = ConsumerRecords.empty();
					try {
						records = consumer.poll(5000);
						retryPolicy.succeess();
					} catch (Exception e) {
						m_logger.warn("Pull messages failed!", e);
						retryPolicy.fail(true);
						continue;
					}
					List<ConsumerMessage<?>> msgs = new ArrayList<ConsumerMessage<?>>();
					for (ConsumerRecord<String, byte[]> consumerRecord : records) {
						if (!closed.get()) {
							long offset = -1;
							try {
								offset = consumerRecord.offset();
								ByteBuf byteBuf = Unpooled.wrappedBuffer(consumerRecord.value());

								BaseConsumerMessage<?> baseMsg = m_messageCodec.decode(consumerContext.getTopic().getName(),
								      byteBuf, consumerContext.getMessageClazz());
								@SuppressWarnings("rawtypes")
								ConsumerMessage kafkaMsg = new KafkaConsumerMessage(baseMsg, consumerRecord.partition(),
								      consumerRecord.offset());
								msgs.add(kafkaMsg);
							} catch (Exception e) {
								m_logger
								      .warn("Kafka consumer failed Topic:{} Partition:{} Offset:{} Group:{} SesssionId:{} Exception:{}",
								            consumerRecord.topic(), consumerRecord.partition(), offset,
								            consumerContext.getGroupId(), consumerContext.getSessionId(), e.getMessage());
							}
						}
					}
					if (msgs.size() > 0 && !closed.get())
						m_consumerNotifier.messageReceived(token, msgs);
				}
			} catch (WakeupException e) {
				if (!closed.get())
					throw e;
			} catch (Exception e) {
				if (!closed.get()) {
					m_logger.error("Consumer exited abnormally.", e);
					throw e;
				}
			} finally {
				m_logger.info("Closing kafka consumer with token: " + token);
				Set<TopicPartition> assignment = consumer.assignment();
				consumer.close();
				m_logger.info("Closed assignment: " + assignment);
			}
		}

		public void shutdown() {
			closed.set(true);
			consumer.wakeup();
		}

		public Properties getProps() {
			return props;
		}

	}

	private Properties getConsumerProperties(String topic, String group) {
		Properties configs = KafkaProperties.getDefaultKafkaConsumerProperties();

		try {
			Properties envProperties = m_environment.getConsumerConfig(topic);
			configs.putAll(envProperties);
		} catch (IOException e) {
			m_logger.warn("kafka read consumer config failed", e);
		}

		List<Partition> partitions = m_metaService.listPartitionsByTopic(topic);
		if (partitions == null || partitions.size() < 1) {
			return configs;
		}

		String consumerDatasource = partitions.get(0).getReadDatasource();
		Storage targetStorage = m_metaService.findStorageByTopic(topic);
		if (targetStorage == null) {
			return configs;
		}

		String targetIdc = getTargetIdc(m_metaService.findTopicByName(topic), group).toLowerCase();
		String targetZk = String.format("%s.%s", KafkaConstants.ZOOKEEPER_CONNECT_PROPERTY_NAME, targetIdc);
		String targetBootstrapServers = String.format("%s.%s", KafkaConstants.BOOTSTRAP_SERVERS_PROPERTY_NAME, targetIdc);

		for (Datasource datasource : targetStorage.getDatasources()) {
			if (consumerDatasource.equals(datasource.getId())) {
				Map<String, Property> properties = datasource.getProperties();
				for (Map.Entry<String, Property> prop : properties.entrySet()) {
					String propName = prop.getValue().getName();
					if (!propName.startsWith(KafkaConstants.ZOOKEEPER_CONNECT_PROPERTY_NAME)
					      && !(propName.startsWith(KafkaConstants.BOOTSTRAP_SERVERS_PROPERTY_NAME))) {
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

				if (properties.get(targetZk) != null) {
					configs.put(KafkaConstants.ZOOKEEPER_CONNECT_PROPERTY_NAME, properties.get(targetZk).getValue());
				} else {
					configs.put(KafkaConstants.ZOOKEEPER_CONNECT_PROPERTY_NAME,
					      properties.get(KafkaConstants.ZOOKEEPER_CONNECT_PROPERTY_NAME).getValue());
				}

				break;
			}
		}
		configs.put("group.id", group);
		return KafkaProperties.overrideByCtripDefaultConsumerSetting(configs, topic, group, targetIdc);
	}

	private String getTargetIdc(Topic topic, String consumerGroup) {
		String idcPolicy = topic.findConsumerGroup(consumerGroup).getIdcPolicy();
		if (idcPolicy == null) {
			idcPolicy = IdcPolicy.LOCAL;
		}

		String targetIdc = m_kafkaIdcStrategy.getTargetIdc(idcPolicy);
		if (targetIdc == null) {
			throw new RuntimeException("Can not get target idc!");
		}

		return targetIdc;
	}

	@Override
	public void initialize() throws InitializationException {
		Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create("CheckIdcPolicyChangesExecutor", true))
		      .scheduleWithFixedDelay(new Runnable() {
			      @Override
			      public void run() {
				      try {
					      for (Entry<ConsumerContext, KafkaConsumerThread> entry : consumers.entrySet()) {
						      Properties newConsumerProperties = getConsumerProperties(entry.getKey().getTopic().getName(),
						            entry.getKey().getGroup().getName());
						      Properties currentConsumerProperties = entry.getValue().getProps();

						      String newZookeeperConnectProperty = newConsumerProperties.getProperty(KafkaConstants.ZOOKEEPER_CONNECT_PROPERTY_NAME);
						      String currentZookeeperConnectProperty = currentConsumerProperties
						            .getProperty(KafkaConstants.ZOOKEEPER_CONNECT_PROPERTY_NAME);
						      String newBootstrapServersProperty = newConsumerProperties
						            .getProperty(KafkaConstants.BOOTSTRAP_SERVERS_PROPERTY_NAME);
						      String currentBootstrapServersProperty = currentConsumerProperties
						            .getProperty(KafkaConstants.BOOTSTRAP_SERVERS_PROPERTY_NAME);

						      if (!(newZookeeperConnectProperty != null && currentConsumerProperties != null
						            && newZookeeperConnectProperty.equals(currentZookeeperConnectProperty)
						            && newBootstrapServersProperty != null && currentBootstrapServersProperty != null && newBootstrapServersProperty
						            .equals(currentBootstrapServersProperty))) {
							      synchronized (consumers) {
								      if (consumers.containsKey(entry.getKey())) {
									      m_logger.info("Restart consumer:{}, topic:{}, as target kafka cluster changed to {}.",
									            entry.getKey().getGroupId(), entry.getKey().getTopic().getName(),
									            newBootstrapServersProperty);
									      restart(entry.getKey());
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
