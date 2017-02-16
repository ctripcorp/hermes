package com.ctrip.hermes.producer;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.constants.CatConstants;
import com.ctrip.hermes.core.exception.MessageSendException;
import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.pipeline.Pipeline;
import com.ctrip.hermes.core.result.CompletionCallback;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.producer.api.Producer;
import com.ctrip.hermes.producer.build.BuildConstants;
import com.ctrip.hermes.producer.config.ProducerConfig;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;

@Named(type = Producer.class)
public class DefaultProducer extends Producer {

	private static final Logger log = LoggerFactory.getLogger(DefaultProducer.class);

	@Inject(BuildConstants.PRODUCER)
	private Pipeline<Future<SendResult>> m_pipeline;

	@Inject
	private SystemClockService m_systemClockService;

	@Inject
	private MetaService m_metaService;

	@Inject
	private ProducerConfig m_config;

	@Override
	public DefaultMessageHolder message(String topic, String partitionKey, Object body) {
		if (StringUtils.isBlank(topic)) {
			throw new IllegalArgumentException("Topic can not be null or empty.");
		}
		return new DefaultMessageHolder(topic, partitionKey, body);
	}

	class DefaultMessageHolder implements MessageHolder {
		private ProducerMessage<Object> m_msg;

		public DefaultMessageHolder(String topic, String partitionKey, Object body) {
			m_msg = new ProducerMessage<Object>(topic, body);
			m_msg.setPartitionKey(partitionKey);
		}

		@Override
		public Future<SendResult> send() {
			Transaction t = null;
			if (m_config.isCatEnabled()) {
				t = Cat.newTransaction(CatConstants.TYPE_MESSAGE_PRODUCE_TRIED, m_msg.getTopic());
			}
			try {
				Topic topic = m_metaService.findTopicByName(m_msg.getTopic());

				if (topic == null) {
					log.error("Topic {} not found.", m_msg.getTopic());
					throw new IllegalArgumentException(String.format("Topic %s not found.", m_msg.getTopic()));
				}

				if (Storage.KAFKA.equals(topic.getStorageType())) {
					m_msg.setWithCatTrace(false);
				}
				m_msg.setBornTime(m_systemClockService.now());
				Future<SendResult> future = m_pipeline.put(m_msg);

				if (t != null) {
					t.setStatus(Transaction.SUCCESS);
				}
				return future;
			} catch (Exception e) {
				if (t != null) {
					Cat.logError(e);
					t.setStatus(e);
				}
				throw e;
			} finally {
				if (t != null) {
					t.complete();
				}
			}
		}

		@Override
		public MessageHolder withRefKey(String key) {
			if (key != null && key.length() > 90) {
				throw new IllegalArgumentException(String.format(
				      "RefKey's length must not larger than 90 characters(refKey=%s)", key));
			}

			m_msg.setKey(key);
			return this;
		}

		@Override
		public MessageHolder withPriority() {
			m_msg.setPriority(true);
			return this;
		}

		@Override
		public MessageHolder addProperty(String key, String value) {
			m_msg.addDurableAppProperty(key, value);
			return this;
		}

		@Override
		public MessageHolder setCallback(CompletionCallback<SendResult> callback) {
			m_msg.setCallback(callback);
			return this;
		}

		@Override
		public SendResult sendSync() throws MessageSendException {
			try {
				return send().get();
			} catch (ExecutionException e) {
				throw new MessageSendException(e, m_msg);
			} catch (InterruptedException e) {
				throw new MessageSendException(e, m_msg);
			}
		}

	}
}
