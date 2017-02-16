package com.ctrip.hermes.kafka.avro;

import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.ctrip.hermes.consumer.api.Consumer;
import com.ctrip.hermes.consumer.api.Consumer.ConsumerHolder;
import com.ctrip.hermes.consumer.api.MessageListener;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.message.payload.assist.HermesKafkaAvroDeserializer;
import com.ctrip.hermes.core.message.payload.assist.HermesKafkaAvroSerializer;
import com.ctrip.hermes.core.message.payload.assist.SchemaRegisterRestClient;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.kafka.producer.KafkaMessageSender;
import com.ctrip.hermes.kafka.producer.KafkaSendResult;
import com.ctrip.hermes.kafka.server.MockKafkaCluster;
import com.ctrip.hermes.kafka.server.MockZookeeper;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.producer.api.Producer;
import com.ctrip.hermes.producer.api.Producer.MessageHolder;
import com.ctrip.hermes.producer.sender.MessageSender;

public class KafkaAvroTest {

	private static MockZookeeper zk;

	private static MockKafkaCluster kafkaCluster;

	private static String topic = "kafka.SimpleAvroTopic";

	@BeforeClass
	public static void beforeClass() {
		zk = new MockZookeeper();
		kafkaCluster = new MockKafkaCluster(zk, 3);
		kafkaCluster.createTopic(topic, 3, 1);
	}

	@AfterClass
	public static void afterClass() {
		KafkaMessageSender kafkaSender = (KafkaMessageSender) PlexusComponentLocator.lookup(MessageSender.class,
		      Endpoint.KAFKA);
		kafkaSender.close();
		if (kafkaCluster != null) {
			kafkaCluster.stop();
		}
		if (zk != null) {
			zk.stop();
		}
	}

	@Test
	public void testByBatch() throws InterruptedException, ExecutionException, IOException, RestClientException {
		String group = "SimpleAvroTopicGroup";

		int schemaId = 100;
		String schemaString = AvroVisitEvent.getClassSchema().toString();

		SchemaRegisterRestClient restService = Mockito.mock(SchemaRegisterRestClient.class);
		Mockito.when(restService.registerSchema(Mockito.anyString(), Mockito.anyString())).thenReturn(schemaId);
		Mockito.when(restService.getId(Mockito.anyInt())).thenReturn(new SchemaString(schemaString));

		HermesKafkaAvroSerializer serializer = PlexusComponentLocator.lookup(HermesKafkaAvroSerializer.class);
		HermesKafkaAvroDeserializer deserializer = PlexusComponentLocator.lookup(HermesKafkaAvroDeserializer.class);
		serializer.setSchemaRestService(restService);
		deserializer.setSchemaRestService(restService);

		Producer producer = Producer.getInstance();

		final List<AvroVisitEvent> actualResult = new ArrayList<AvroVisitEvent>();
		final List<AvroVisitEvent> expectedResult = new ArrayList<AvroVisitEvent>();

		ConsumerHolder consumerHolder = Consumer.getInstance().start(topic, group, new MessageListener<AvroVisitEvent>() {

			@Override
			public void onMessage(List<ConsumerMessage<AvroVisitEvent>> msgs) {
				for (ConsumerMessage<AvroVisitEvent> msg : msgs) {
					AvroVisitEvent event = msg.getBody();
					System.out.println("Consumer Received: " + event);
					actualResult.add(event);
				}
			}
		});

		System.out.println("Starting consumer...");
		Thread.sleep(15000);

		int i = 0;
		while (i++ < 10) {
			AvroVisitEvent event = generateEvent();
			MessageHolder holder = producer.message(topic, null, event);
			Future<SendResult> future = holder.send();
			KafkaSendResult sendResult = (KafkaSendResult) future.get();
			System.out.format("Producer Sent: %s Partition:%s Offset:%s%n", event, sendResult.getPartition(),
			      sendResult.getOffset());
			expectedResult.add(event);
		}

		int sleepCount = 0;
		while (actualResult.size() < expectedResult.size() && sleepCount++ < 50) {
			Thread.sleep(100);
		}

		Assert.assertEquals(expectedResult.size(), actualResult.size());
		consumerHolder.close();
	}

	static AtomicLong counter = new AtomicLong();

	public static AvroVisitEvent generateEvent() {
		Random random = new Random(System.currentTimeMillis());
		AvroVisitEvent event = AvroVisitEvent.newBuilder().setIp("192.168.0." + random.nextInt(255))
		      .setTz(System.currentTimeMillis()).setUrl("www.ctrip.com/" + counter.incrementAndGet()).build();
		return event;
	}
}
