package com.ctrip.hermes.kafka;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.WakeupException;
import org.junit.Assert;
import org.junit.Test;

import com.ctrip.hermes.kafka.avro.KafkaAvroTest;

public class NativeKafkaWithAvroDecoderTest {

	@Test
	public void testNative() throws IOException, InterruptedException, ExecutionException {
		final String topic = "kafka.SimpleAvroTopic";
		int msgNum = 200;
		final CountDownLatch countDown = new CountDownLatch(msgNum);

		Properties producerProps = new Properties();
		producerProps.put("bootstrap.servers", "");

		// Avro Decoder/Encoder
		CachedSchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient("",
		      AbstractKafkaAvroSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT);
		Map<String, String> configs = new HashMap<String, String>();
		configs.put("schema.registry.url", "");

		KafkaAvroSerializer avroKeySerializer = new KafkaAvroSerializer();
		avroKeySerializer.configure(configs, true);
		KafkaAvroSerializer avroValueSerializer = new KafkaAvroSerializer();
		avroValueSerializer.configure(configs, false);

		Map<String, String> deserializerConfigs = new HashMap<String, String>();
		deserializerConfigs.put("specific.avro.reader", Boolean.TRUE.toString());
		deserializerConfigs.put("schema.registry.url", "");
		KafkaAvroDeserializer avroKeyDeserializer = new KafkaAvroDeserializer(schemaRegistry, deserializerConfigs);
		avroKeyDeserializer.configure(configs, true);
		KafkaAvroDeserializer avroValueDeserializer = new KafkaAvroDeserializer(schemaRegistry, deserializerConfigs);
		avroValueDeserializer.configure(configs, false);

		// Consumer
		final Properties consumerProps = new Properties();
		consumerProps.put("bootstrap.servers", "");
		consumerProps.put("group.id", "GROUP_" + topic);

		final List<Object> actualResult = new ArrayList<Object>();
		final List<Object> expectedResult = new ArrayList<Object>();

		final KafkaConsumer<Object, Object> consumer = new KafkaConsumer<Object, Object>(consumerProps,
		      avroKeyDeserializer, avroValueDeserializer);
		consumer.subscribe(Arrays.asList(topic));

		class KafkaConsumerThread implements Runnable {

			private final AtomicBoolean closed = new AtomicBoolean(false);

			public void run() {
				try {
					while (!closed.get()) {
						ConsumerRecords<Object, Object> records = consumer.poll(100);
						for (ConsumerRecord<Object, Object> consumerRecord : records) {
							System.out.println("received: " + consumerRecord.value());
							actualResult.add(consumerRecord.value());
							countDown.countDown();
						}
					}
				} catch (WakeupException e) {
					if (!closed.get())
						throw e;
				} finally {
					consumer.commitSync();
					consumer.close();
				}
			}

			public void shutdown() {
				closed.set(true);
				consumer.wakeup();
			}
		}

		KafkaConsumerThread thread = new KafkaConsumerThread();
		new Thread(thread).start();

		KafkaProducer<Object, Object> producer = new KafkaProducer<Object, Object>(producerProps, avroKeySerializer,
		      avroValueSerializer);
		int i = 0;
		while (i++ < msgNum) {
			ProducerRecord<Object, Object> data = new ProducerRecord<Object, Object>(topic, null,
			      (Object) KafkaAvroTest.generateEvent());
			Future<RecordMetadata> send = producer.send(data);
			send.get();
			if (send.isDone()) {
				System.out.println("sending: " + data.value());
				expectedResult.add(data.value());
			}
		}

		countDown.await();

		thread.shutdown();
		producer.close();

		Assert.assertEquals(expectedResult.size(), actualResult.size());
	}
}
