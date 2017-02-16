package com.ctrip.hermes.core.message.payload.assist;

import java.util.Map;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.message.payload.assist.kafka.avro.AbstractKafkaAvroSerializer;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

@Named(type = HermesKafkaAvroSerializer.class)
public class HermesKafkaAvroSerializer extends AbstractKafkaAvroSerializer {
	private boolean isKey;

	@Inject
	private SchemaRegisterRestClient m_schemaRestService;

	public void setSchemaRestService(SchemaRegisterRestClient restService) {
		m_schemaRestService = restService;
	}

	public void configure(Map<String, ?> configs, boolean isKey) {
		this.isKey = isKey;
		Object maxSchema = configs.get(AbstractKafkaAvroSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_CONFIG);
		maxSchema = maxSchema == null ? AbstractKafkaAvroSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT : maxSchema;
		schemaRegistry = new CachedSchemaRegistryClient(m_schemaRestService, (Integer) maxSchema);
	}

	public byte[] serialize(String topic, Object record) {
		String subject;
		if (isKey) {
			subject = topic + "-key";
		} else {
			subject = topic + "-value";
		}
		return serializeImpl(subject, record);
	}
}
