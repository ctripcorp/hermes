package com.ctrip.hermes.core.transport.command;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.junit.Test;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.HermesCoreBaseTest;
import com.ctrip.hermes.core.exception.MessageSendException;
import com.ctrip.hermes.core.message.PartialDecodedMessage;
import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.message.PropertiesHolder;
import com.ctrip.hermes.core.message.payload.JsonPayloadCodec;
import com.ctrip.hermes.core.result.SendResult;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class SendMessageCommandTest extends HermesCoreBaseTest {

	@Test
	public void test() throws Exception {
		String topic = "topic";
		long bornTime = System.currentTimeMillis();
		SendMessageCommand cmd = new SendMessageCommand("topic", 10);
		ProducerMessage<String> pmsg1 = createProducerMessage(topic, "body1", "key1", 10, "pKey1", bornTime, true, true,
		      Arrays.asList(new Pair<String, String>("a1", "A1")), Arrays.asList(new Pair<String, String>("b1", "B1")),
		      Arrays.asList(new Pair<String, String>("c1", "C1")));

		SettableFuture<SendResult> future1 = SettableFuture.create();
		cmd.addMessage(pmsg1, future1);

		ProducerMessage<String> pmsg2 = createProducerMessage(topic, "body2", "key2", 10, "pKey2", bornTime, true, true,
		      Arrays.asList(new Pair<String, String>("a2", "A2")), Arrays.asList(new Pair<String, String>("b2", "B2")),
		      Arrays.asList(new Pair<String, String>("c2", "C2")));

		SettableFuture<SendResult> future2 = SettableFuture.create();
		cmd.addMessage(pmsg2, future2);

		ProducerMessage<String> pmsg3 = createProducerMessage(topic, "body3", "key3", 10, "pKey3", bornTime, false, true,
		      Arrays.asList(new Pair<String, String>("a3", "A3")), Arrays.asList(new Pair<String, String>("b3", "B3")),
		      Arrays.asList(new Pair<String, String>("c3", "C3")));

		SettableFuture<SendResult> future3 = SettableFuture.create();
		cmd.addMessage(pmsg3, future3);

		ProducerMessage<String> pmsg4 = createProducerMessage(topic, "body4", "key4", 10, "pKey4", bornTime, false, true,
		      Arrays.asList(new Pair<String, String>("a4", "A4")), Arrays.asList(new Pair<String, String>("b4", "B4")),
		      Arrays.asList(new Pair<String, String>("c4", "C4")));

		SettableFuture<SendResult> future4 = SettableFuture.create();
		cmd.addMessage(pmsg4, future4);

		ByteBuf buf = Unpooled.buffer();
		cmd.toBytes(buf);

		SendMessageCommand decodedCmd = new SendMessageCommand();
		Header header = new Header();
		header.parse(buf);
		decodedCmd.parse(buf, header);

		assertEquals(4, decodedCmd.getMessageCount());
		assertEquals(10, decodedCmd.getPartition());
		assertEquals(topic, decodedCmd.getTopic());
		assertEquals(0, decodedCmd.getProducerMessages().size());
		assertEquals(cmd.getHeader().getCorrelationId(), decodedCmd.getHeader().getCorrelationId());
		Map<Integer, MessageBatchWithRawData> batches = decodedCmd.getMessageRawDataBatches();
		assertEquals(2, batches.size());
		MessageBatchWithRawData priorityData = batches.get(0);
		assertRawData(priorityData, topic, Arrays.asList(0, 1), Arrays.asList(pmsg1, pmsg2));

		MessageBatchWithRawData nonPriorityData = batches.get(1);
		assertRawData(nonPriorityData, topic, Arrays.asList(2, 3), Arrays.asList(pmsg3, pmsg4));

	}

	@Test
	public void testGetProducerMessageFuturePairs() throws Exception {
		String topic = "topic";
		long bornTime = System.currentTimeMillis();
		SendMessageCommand cmd = new SendMessageCommand("topic", 10);
		ProducerMessage<String> pmsg1 = createProducerMessage(topic, "body1", "key1", 10, "pKey1", bornTime, true, true,
		      Arrays.asList(new Pair<String, String>("a1", "A1")), Arrays.asList(new Pair<String, String>("b1", "B1")),
		      Arrays.asList(new Pair<String, String>("c1", "C1")));

		SettableFuture<SendResult> future1 = SettableFuture.create();
		cmd.addMessage(pmsg1, future1);

		ProducerMessage<String> pmsg2 = createProducerMessage(topic, "body2", "key2", 10, "pKey2", bornTime, true, true,
		      Arrays.asList(new Pair<String, String>("a2", "A2")), Arrays.asList(new Pair<String, String>("b2", "B2")),
		      Arrays.asList(new Pair<String, String>("c2", "C2")));

		SettableFuture<SendResult> future2 = SettableFuture.create();
		cmd.addMessage(pmsg2, future2);

		ProducerMessage<String> pmsg3 = createProducerMessage(topic, "body3", "key3", 10, "pKey3", bornTime, false, true,
		      Arrays.asList(new Pair<String, String>("a3", "A3")), Arrays.asList(new Pair<String, String>("b3", "B3")),
		      Arrays.asList(new Pair<String, String>("c3", "C3")));

		SettableFuture<SendResult> future3 = SettableFuture.create();
		cmd.addMessage(pmsg3, future3);

		ProducerMessage<String> pmsg4 = createProducerMessage(topic, "body4", "key4", 10, "pKey4", bornTime, false, true,
		      Arrays.asList(new Pair<String, String>("a4", "A4")), Arrays.asList(new Pair<String, String>("b4", "B4")),
		      Arrays.asList(new Pair<String, String>("c4", "C4")));

		SettableFuture<SendResult> future4 = SettableFuture.create();
		cmd.addMessage(pmsg4, future4);

		List<Pair<ProducerMessage<?>, SettableFuture<SendResult>>> pairs = cmd.getProducerMessageFuturePairs();

		assertEquals(4, pairs.size());

		assertSame(pmsg1, pairs.get(0).getKey());
		assertSame(future1, pairs.get(0).getValue());
		assertSame(pmsg2, pairs.get(1).getKey());
		assertSame(future2, pairs.get(1).getValue());
		assertSame(pmsg3, pairs.get(2).getKey());
		assertSame(future3, pairs.get(2).getValue());
		assertSame(pmsg4, pairs.get(3).getKey());
		assertSame(future4, pairs.get(3).getValue());

	}

	@Test
	public void testOnResultReceivedSuccess() throws Exception {
		SendMessageCommand cmd = new SendMessageCommand("test", 1);

		SettableFuture<SendResult> future = SettableFuture.create();
		ProducerMessage<String> msg = new ProducerMessage<String>("test", "hello");
		msg.setPartition(1);
		cmd.addMessage(msg, future);

		SendMessageResultCommand resultCmd = new SendMessageResultCommand();
		Map<Integer, Boolean> results = new HashMap<Integer, Boolean>();
		results.put(0, true);
		resultCmd.addResults(results);
		cmd.onResultReceived(resultCmd);

		assertTrue(future.isDone());
		assertNotNull(future.get());
	}

	@Test
	public void testOnResultReceivedFail() throws Exception {
		SendMessageCommand cmd = new SendMessageCommand("test", 1);

		SettableFuture<SendResult> future = SettableFuture.create();
		ProducerMessage<String> msg = new ProducerMessage<String>("test", "hello");
		msg.setPartition(1);
		cmd.addMessage(msg, future);

		SendMessageResultCommand resultCmd = new SendMessageResultCommand();
		Map<Integer, Boolean> results = new HashMap<Integer, Boolean>();
		results.put(0, false);
		resultCmd.addResults(results);
		cmd.onResultReceived(resultCmd);

		assertTrue(future.isDone());

		try {
			future.get();
			fail();
		} catch (ExecutionException e) {
			if (!(e.getCause() instanceof MessageSendException)) {
				fail();
			}
		} catch (Exception e) {
			fail();
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void testTopicNotMatch() throws Exception {
		String topic = "topic";
		long bornTime = System.currentTimeMillis();
		SendMessageCommand cmd = new SendMessageCommand("topic", 10);
		ProducerMessage<String> pmsg1 = createProducerMessage(topic + "1111", "body1", "key1", 10, "pKey1", bornTime,
		      true, true, Arrays.asList(new Pair<String, String>("a1", "A1")),
		      Arrays.asList(new Pair<String, String>("b1", "B1")), Arrays.asList(new Pair<String, String>("c1", "C1")));

		SettableFuture<SendResult> future1 = SettableFuture.create();
		cmd.addMessage(pmsg1, future1);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testPartitionNotMatch() throws Exception {
		String topic = "topic";
		long bornTime = System.currentTimeMillis();
		SendMessageCommand cmd = new SendMessageCommand("topic", 10);
		ProducerMessage<String> pmsg1 = createProducerMessage(topic, "body1", "key1", 101, "pKey1", bornTime, true, true,
		      Arrays.asList(new Pair<String, String>("a1", "A1")), Arrays.asList(new Pair<String, String>("b1", "B1")),
		      Arrays.asList(new Pair<String, String>("c1", "C1")));

		SettableFuture<SendResult> future1 = SettableFuture.create();
		cmd.addMessage(pmsg1, future1);
	}

	private void assertRawData(MessageBatchWithRawData actual, String topic, List<Integer> seqs,
	      List<ProducerMessage<String>> pmsgs) {
		List<Integer> actualSeqs = actual.getMsgSeqs();
		assertEquals(seqs, actualSeqs);
		assertEquals(topic, actual.getTopic());
		List<PartialDecodedMessage> actualMsgs = actual.getMessages();
		assertEquals(pmsgs.size(), actualMsgs.size());
		for (int i = 0; i < pmsgs.size(); i++) {
			PartialDecodedMessage actualMsg = actualMsgs.get(i);
			ProducerMessage<?> expectedMsg = pmsgs.get(i);
			assertEquals(expectedMsg.getBody(), new JsonPayloadCodec().decode(actualMsg.readBody(), String.class));
			assertEquals(expectedMsg.getKey(), actualMsg.getKey());
			assertEquals(expectedMsg.getPropertiesHolder().getDurableProperties(),
			      readProperties(actualMsg.getDurableProperties()));
			assertEquals(expectedMsg.getPropertiesHolder().getVolatileProperties(),
			      readProperties(actualMsg.getVolatileProperties()));
		}
	}

	private ProducerMessage<String> createProducerMessage(String topic, String body, String key, int partition,
	      String partitionKey, long bornTime, boolean isPriority, boolean withHeader,
	      List<Pair<String, String>> appProperites, List<Pair<String, String>> sysProperites,
	      List<Pair<String, String>> volatileProperites) {
		ProducerMessage<String> msg = new ProducerMessage<String>(topic, body);
		msg.setBornTime(bornTime);
		msg.setKey(key);
		msg.setPartition(partition);
		msg.setPartitionKey(partitionKey);
		msg.setPriority(isPriority);
		msg.setWithCatTrace(withHeader);
		PropertiesHolder propertiesHolder = new PropertiesHolder();
		if (appProperites != null && !appProperites.isEmpty()) {
			for (Pair<String, String> appProperty : appProperites) {
				propertiesHolder.addDurableAppProperty(appProperty.getKey(), appProperty.getValue());
			}
		}
		if (sysProperites != null && !sysProperites.isEmpty()) {
			for (Pair<String, String> sysProperty : sysProperites) {
				propertiesHolder.addDurableSysProperty(sysProperty.getKey(), sysProperty.getValue());
			}
		}
		if (volatileProperites != null && !volatileProperites.isEmpty()) {
			for (Pair<String, String> volatileProperty : volatileProperites) {
				propertiesHolder.addVolatileProperty(volatileProperty.getKey(), volatileProperty.getValue());
			}
		}
		msg.setPropertiesHolder(propertiesHolder);
		return msg;
	}
}
