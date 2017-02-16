package com.ctrip.hermes.core.message.payload;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.message.payload.AbstractPayloadCodec;
import com.ctrip.hermes.core.message.payload.PayloadCodec;
import com.ctrip.hermes.core.message.payload.assist.HermesKafkaAvroDeserializer;
import com.ctrip.hermes.core.message.payload.assist.HermesKafkaAvroSerializer;

@Named(type = PayloadCodec.class, value = com.ctrip.hermes.meta.entity.Codec.AVRO)
public class AvroPayloadCodec extends AbstractPayloadCodec implements Initializable {
	private static final Logger log = LoggerFactory.getLogger(AvroPayloadCodec.class);

	static final String SCHEMA_REGISTRY_URL = "schema.registry.url";

	@Inject
	private HermesKafkaAvroSerializer avroSerializer;

	@Inject
	private HermesKafkaAvroDeserializer specificDeserializer;

	@Inject(value = "GENERIC")
	private HermesKafkaAvroDeserializer genericDeserializer;

	@SuppressWarnings("unchecked")
	@Override
	public <T> T doDecode(byte[] raw, Class<T> clazz) {
		return (T) (clazz == GenericRecord.class ? genericDeserializer : specificDeserializer).deserialize(null, raw);
	}

	@Override
	public byte[] doEncode(String topic, Object obj) {
		try {
			return avroSerializer.serialize(topic, obj);
		} catch (Exception e) {
			log.error("Serialize avro object failed!", e);
			throw e;
		}
	}

	@Override
	public String getType() {
		return com.ctrip.hermes.meta.entity.Codec.AVRO;
	}

	@Override
	public void initialize() throws InitializationException {
		Map<String, String> configs = new HashMap<String, String>();

		avroSerializer.configure(configs, false);

		configs.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, Boolean.TRUE.toString());
		specificDeserializer.configure(configs, false);

		configs.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, Boolean.FALSE.toString());
		genericDeserializer.configure(configs, false);
	}
}
