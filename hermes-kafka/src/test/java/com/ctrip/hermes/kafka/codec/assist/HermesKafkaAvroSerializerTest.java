package com.ctrip.hermes.kafka.codec.assist;

import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.core.message.payload.PayloadCodec;
import com.ctrip.hermes.core.message.payload.assist.HermesKafkaAvroDeserializer;
import com.ctrip.hermes.core.message.payload.assist.HermesKafkaAvroSerializer;
import com.ctrip.hermes.core.message.payload.assist.SchemaRegisterRestClient;
import com.ctrip.hermes.kafka.avro.AvroVisitEvent;
import com.ctrip.hermes.meta.entity.Codec;

public class HermesKafkaAvroSerializerTest extends ComponentTestCase {

	private HermesKafkaAvroSerializer m_serializer;

	private HermesKafkaAvroDeserializer m_deserializer;

	private static final int MOCK_ID = 28;

	@Before
	public void init() throws Exception {
		String schemaString = "{\"namespace\": \"com.ctrip.hermes.kafka.avro\", \"type\": \"record\", \"name\": \"AvroVisitEvent\", \"fields\": [ {\"name\": \"ip\", \"type\": \"string\"}, {\"name\": \"url\", \"type\": \"string\"}, {\"name\": \"tz\", \"type\": \"long\", \"java-class\":\"java.util.Date\"} ] }";

		SchemaRegisterRestClient restService = Mockito.mock(SchemaRegisterRestClient.class);
		Mockito.when(restService.registerSchema(Mockito.anyString(), Mockito.anyString())).thenReturn(MOCK_ID);
		Mockito.when(restService.getId(Mockito.anyInt())).thenReturn(new SchemaString(schemaString));

		m_serializer = new HermesKafkaAvroSerializer();
		m_deserializer = new HermesKafkaAvroDeserializer();

		m_serializer.setSchemaRestService(restService);
		Map<String, String> configs = new HashMap<String, String>();
		m_serializer.configure(configs, false);
		configs.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, Boolean.TRUE.toString());
		m_deserializer.setSchemaRestService(restService);
		m_deserializer.configure(configs, false);
	}

	@Test
	public void testSerialize() {
		byte[] serialize = m_serializer.serialize("topic", "content");
		Assert.assertEquals(serialize[4], MOCK_ID);
		Assert.assertNotEquals("content", new String(serialize));

		byte[] _4str = new byte[serialize.length - 6];
		for (int i = 0; i < _4str.length; i++) {
			_4str[i] = serialize[i + 6];
		}
		Assert.assertEquals(new String("content"), new String(_4str));
	}

	@Test
	public void testDeserialize() throws Exception {
		AvroVisitEvent event = new AvroVisitEvent();
		event.setIp("123.123.123.123");
		event.setTz(12345678901234567L);
		event.setUrl("http://127.0.0.1");

		byte[] serialized = m_serializer.serialize("topic", event);
		AvroVisitEvent e = (AvroVisitEvent) m_deserializer.deserialize("no use", serialized);
		Assert.assertEquals(event.getIp(), String.valueOf(e.getIp()));
		Assert.assertEquals(event.getTz(), e.getTz());
		Assert.assertEquals(event.getUrl(), String.valueOf(e.getUrl()));
	}

	@Test
	public void testRealSerialize() {
		AvroVisitEvent event = new AvroVisitEvent();
		event.setIp("123.123.123.123");
		event.setTz(12345678901234567L);
		event.setUrl("http://127.0.0.1");

		PayloadCodec codec = lookup(PayloadCodec.class, Codec.AVRO);
		byte[] serialized = codec.encode("topic-songtong", event);
		AvroVisitEvent e = codec.decode(serialized, AvroVisitEvent.class);
		System.out.println(e);
		Assert.assertEquals(event.getIp(), String.valueOf(e.getIp()));
		Assert.assertEquals(event.getTz(), e.getTz());
		Assert.assertEquals(event.getUrl(), String.valueOf(e.getUrl()));
	}
}
