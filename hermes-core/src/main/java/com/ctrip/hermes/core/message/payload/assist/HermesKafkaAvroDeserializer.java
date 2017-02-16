package com.ctrip.hermes.core.message.payload.assist;

import java.util.HashMap;
import java.util.Map;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.message.payload.assist.kafka.avro.AbstractKafkaAvroDeserializer;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

@Named(type = HermesKafkaAvroDeserializer.class)
public class HermesKafkaAvroDeserializer extends AbstractKafkaAvroDeserializer {
	@Inject
	private SchemaRegisterRestClient m_schemaRestService;

	public void setSchemaRestService(SchemaRegisterRestClient restService) {
		m_schemaRestService = restService;
	}

	@SuppressWarnings("unchecked")
	public void configure(Map<String, ?> configs, boolean isKey) {
		Map<String, String> m = new HashMap<String, String>();
		// faked, KafkaAvroDeserializerConfig need this property
		m.put("schema.registry.url", "http://xxx.xxx.xxx.xxx");
		m.putAll((Map<? extends String, ? extends String>) configs);

		KafkaAvroDeserializerConfig config = new KafkaAvroDeserializerConfig(m);
		try {
			int maxSchemaObject = config.getInt(KafkaAvroDeserializerConfig.MAX_SCHEMAS_PER_SUBJECT_CONFIG);
			schemaRegistry = new CachedSchemaRegistryClient(m_schemaRestService, maxSchemaObject);
			configureNonClientProperties(config);
		} catch (io.confluent.common.config.ConfigException e) {
			throw new RuntimeException(e.getMessage());
		}
	}

	public Object deserialize(String s, byte[] bytes) {
		return deserialize(bytes);
	}
}
