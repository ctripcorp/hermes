package com.ctrip.hermes.kafka.producer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ctrip.hermes.core.result.CompletionCallback;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.kafka.server.MockKafkaCluster;
import com.ctrip.hermes.kafka.server.MockZookeeper;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.producer.api.Producer;
import com.ctrip.hermes.producer.api.Producer.MessageHolder;
import com.ctrip.hermes.producer.sender.MessageSender;

public class ProducerTest {

	private static MockZookeeper zk;

	private static MockKafkaCluster kafkaCluster;

	private static String topic = "kafka.SimpleTextTopic5";

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
		
		kafkaCluster.stop();
		zk.stop();
	}

	@Test
	public void producerSyncTest() throws InterruptedException, ExecutionException {
		List<String> expected = new ArrayList<String>();
		expected.add("msg1");
		expected.add("msg2");
		expected.add("msg3");
		expected.add("msg4");
		expected.add("msg5");
		expected.add("msg6");

		List<SendResult> sendResults = new ArrayList<SendResult>();

		Producer producer = Producer.getInstance();

		for (int i = 0; i < expected.size(); i++) {
			String proMsg = expected.get(i);

			MessageHolder holder = producer.message(topic, String.valueOf(i), proMsg);
			if (System.currentTimeMillis() % 2 == 0) {
				holder = holder.withPriority();
			}
			holder = holder.withRefKey(String.valueOf(i));
			KafkaFuture future = (KafkaFuture) holder.send();
			KafkaSendResult result = future.get();
			Assert.assertTrue(future.isDone());
			sendResults.add(result);
			System.out.println(String.format("Sent:%s, Topic:%s, Partition:%s, Offset:%s", proMsg, result.getTopic(),
			      result.getPartition(), result.getOffset()));
		}

		Assert.assertEquals(expected.size(), sendResults.size());
	}

	@Test
	public void producerWithCallbackTest() throws InterruptedException, ExecutionException {
		List<String> expected = new ArrayList<String>();
		expected.add("msg1");
		expected.add("msg2");
		expected.add("msg3");
		expected.add("msg4");
		expected.add("msg5");
		expected.add("msg6");

		final List<SendResult> sendResults = new ArrayList<SendResult>();

		Producer producer = Producer.getInstance();

		for (int i = 0; i < expected.size(); i++) {
			String proMsg = expected.get(i);

			MessageHolder holder = producer.message(topic, String.valueOf(i), proMsg);
			holder.setCallback(new CompletionCallback<SendResult>() {

				@Override
				public void onSuccess(SendResult sendResult) {
					if (sendResult instanceof KafkaSendResult) {
						KafkaSendResult result = (KafkaSendResult) sendResult;
						sendResults.add(result);
					}
				}

				@Override
				public void onFailure(Throwable t) {

				}

			});
			if (System.currentTimeMillis() % 2 == 0) {
				holder = holder.withPriority();
			}
			holder = holder.withRefKey(String.valueOf(i));
			KafkaFuture future = (KafkaFuture) holder.send();
			KafkaSendResult result = future.get();
			Assert.assertTrue(future.isDone());
			System.out.println(String.format("Sent:%s, Topic:%s, Partition:%s, Offset:%s", proMsg, result.getTopic(),
			      result.getPartition(), result.getOffset()));
		}

	}

}
