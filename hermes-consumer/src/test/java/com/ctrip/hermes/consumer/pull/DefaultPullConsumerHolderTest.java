package com.ctrip.hermes.consumer.pull;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.ctrip.hermes.consumer.api.OffsetAndMetadata;
import com.ctrip.hermes.consumer.api.OffsetCommitCallback;
import com.ctrip.hermes.consumer.api.PullConsumerConfig;
import com.ctrip.hermes.consumer.api.PulledBatch;
import com.ctrip.hermes.consumer.api.TopicPartition;
import com.ctrip.hermes.consumer.engine.ack.AckManager;
import com.ctrip.hermes.consumer.engine.config.ConsumerConfig;
import com.ctrip.hermes.consumer.message.BrokerConsumerMessage;
import com.ctrip.hermes.core.bo.AckContext;
import com.ctrip.hermes.core.message.BaseConsumerMessage;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.transport.command.v5.AckMessageCommandV5;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;

public class DefaultPullConsumerHolderTest {

	private AckManager ackManager;

	private DefaultPullConsumerHolder<String> holder;

	private PullConsumerConfig config;

	private String topic = "topic1";

	private String groupId = "group1";

	private int partitionCount = 5;

	@Before
	public void before() {
		config = new PullConsumerConfig();
		config.setManualCommitInterval(50);
		ackManager = mock(AckManager.class);

		holder = new DefaultPullConsumerHolder<>(topic, groupId, partitionCount, config, ackManager, null,
		      PlexusComponentLocator.lookup(ConsumerConfig.class));
	}

	@Test
	public void testPollFail() {
		long start = System.currentTimeMillis();
		int timeout = 10;
		int error = 100;

		List<ConsumerMessage<String>> msgs = holder.poll(1, timeout).getMessages();
		assertTrue(System.currentTimeMillis() - start < config.getScanIntervalMax() + timeout + error);
		assertEquals(0, msgs.size());
	}

	@Test
	public void testPollSuccess() {
		int timeout = 10;

		holder.onMessage(makeMessages(0, false, "a", 1));

		long start = System.currentTimeMillis();
		List<ConsumerMessage<String>> msgs = holder.poll(10, timeout).getMessages();
		assertTrue(System.currentTimeMillis() - start < 20);
		assertEquals(1, msgs.size());
		assertEquals("a", msgs.get(0).getBody());
	}

	@Test
	public void testCollectFail() {
		long start = System.currentTimeMillis();
		int timeout = 10;
		int error = 100;

		List<ConsumerMessage<String>> msgs = holder.collect(1, timeout).getMessages();
		assertTrue(System.currentTimeMillis() - start < config.getScanIntervalMax() + timeout + error);
		assertEquals(0, msgs.size());
	}

	@Test
	public void testCollectSuccess1() {
		int timeout = 10;

		holder.onMessage(makeMessages(0, false, "a", 1));

		long start = System.currentTimeMillis();
		List<ConsumerMessage<String>> msgs = holder.collect(1, timeout).getMessages();
		assertTrue(System.currentTimeMillis() - start < 20);
		assertEquals(1, msgs.size());
		assertEquals("a", msgs.get(0).getBody());
	}

	@Test
	public void testCollectSuccess2() throws InterruptedException {
		holder.onMessage(makeMessages(0, false, "a", 1));

		final CountDownLatch latch1 = new CountDownLatch(1);
		final CountDownLatch latch2 = new CountDownLatch(1);
		final AtomicReference<List<ConsumerMessage<String>>> msgs = new AtomicReference<>();
		new Thread() {
			public void run() {
				latch1.countDown();
				msgs.set(holder.collect(2, Integer.MAX_VALUE).getMessages());
				latch2.countDown();
			}
		}.start();

		latch1.await();
		assertFalse(latch2.await(config.getScanIntervalMax() * 3, TimeUnit.MILLISECONDS));
		holder.onMessage(makeMessages(0, false, "b", 1));
		assertTrue(latch2.await(config.getScanIntervalMax() * 2, TimeUnit.MILLISECONDS));

		assertEquals(2, msgs.get().size());
		assertEquals("a", msgs.get().get(0).getBody());
		assertEquals("b", msgs.get().get(1).getBody());
	}

	@Test
	public void testPollNoCommit() throws InterruptedException {
		holder.onMessage(makeMessages(0, false, "a", 1));

		final CountDownLatch latch = new CountDownLatch(1);
		when(ackManager.writeAckToBroker(any(AckMessageCommandV5.class))).then(new Answer<Object>() {

			@Override
			public Object answer(InvocationOnMock invocation) throws Throwable {
				latch.countDown();
				return null;
			}
		});
		List<ConsumerMessage<String>> msgs = holder.poll(1, 10).getMessages();
		assertEquals(1, msgs.size());
		assertFalse(latch.await(config.getScanIntervalMax() * 3, TimeUnit.MILLISECONDS));
	}

	@Test
	public void testPollCommit() throws Exception {
		holder.onMessage(makeMessages(0, false, "a", 1));

		final CountDownLatch latch = new CountDownLatch(1);
		when(ackManager.writeAckToBroker(any(AckMessageCommandV5.class))).then(new Answer<Boolean>() {

			@Override
			public Boolean answer(InvocationOnMock invocation) throws Throwable {
				latch.countDown();
				return true;
			}
		});
		PulledBatch<String> batch = holder.poll(1, 10);
		List<ConsumerMessage<String>> msgs = batch.getMessages();
		batch.commitSync();
		assertEquals(1, msgs.size());
		assertTrue(latch.await(config.getScanIntervalMax() * 3, TimeUnit.MILLISECONDS));
	}

	@Test
	public void testCommitTwice1() throws InterruptedException {
		when(ackManager.writeAckToBroker(any(AckMessageCommandV5.class))).thenReturn(true);
		holder.onMessage(makeMessages(1, false, "a", 1));

		PulledBatch<String> batch = holder.poll(1, 10);
		List<ConsumerMessage<String>> msgs = batch.getMessages();
		assertEquals(1, msgs.size());

		batch.commitSync();

		final AtomicBoolean ackCmdIssued = new AtomicBoolean(false);
		when(ackManager.writeAckToBroker(any(AckMessageCommandV5.class))).thenAnswer(new Answer<Boolean>() {

			@Override
			public Boolean answer(InvocationOnMock invocation) throws Throwable {
				ackCmdIssued.set(true);
				return true;
			}
		});
		batch.commitSync();
		assertFalse(ackCmdIssued.get());
	}

	@Test
	public void testCommitTwoRetrive() throws InterruptedException {
		holder.onMessage(makeMessages(1, false, "a", 10));

		PulledBatch<String> batch1 = holder.poll(5, 10);
		List<ConsumerMessage<String>> msgs = batch1.getMessages();
		assertEquals(1, msgs.size());
		assertEquals(1, msgs.get(0).getPartition());
		assertEquals(10, msgs.get(0).getOffset());
		msgs.get(0).nack();

		holder.onMessage(makeMessages(2, false, "b", 20));
		PulledBatch<String> batch2 = holder.poll(5, 10);
		msgs = batch2.getMessages();
		assertEquals(1, msgs.size());
		assertEquals(2, msgs.get(0).getPartition());
		assertEquals(20, msgs.get(0).getOffset());
		msgs.get(0).nack();

		final CountDownLatch latch = new CountDownLatch(1);
		when(ackManager.writeAckToBroker(any(AckMessageCommandV5.class))).thenAnswer(new Answer<Boolean>() {

			@Override
			public Boolean answer(InvocationOnMock invocation) throws Throwable {
				latch.countDown();
				return true;
			}
		});
		batch2.commitAsync();
		assertFalse(latch.await(config.getManualCommitInterval() * 3, TimeUnit.MILLISECONDS));

		final List<AckMessageCommandV5> cmds = new LinkedList<>();
		final CountDownLatch latch2 = new CountDownLatch(2);
		when(ackManager.writeAckToBroker(any(AckMessageCommandV5.class))).thenAnswer(new Answer<Boolean>() {

			@Override
			public Boolean answer(InvocationOnMock invocation) throws Throwable {
				cmds.add(invocation.getArgumentAt(0, AckMessageCommandV5.class));
				latch2.countDown();
				return true;
			}
		});
		batch1.commitAsync();
		assertTrue(latch2.await(config.getManualCommitInterval() * 3, TimeUnit.MILLISECONDS));

		assertEquals(2, cmds.size());
		HashMap<Integer, AckMessageCommandV5> cmdMap = new HashMap<>();
		for (AckMessageCommandV5 cmd : cmds) {
			cmdMap.put(cmd.getPartition(), cmd);
		}

		int priority = 1;
		AckMessageCommandV5 cmd1 = cmdMap.get(1);
		assertEquals(1, cmd1.getAckedMsgs().get(priority).size());
		assertEquals(10, cmd1.getAckedMsgs().get(priority).get(0).getMsgSeq());
		assertEquals(1, cmd1.getNackedMsgs().get(priority).size());
		assertEquals(10, cmd1.getNackedMsgs().get(priority).get(0).getMsgSeq());

		AckMessageCommandV5 cmd2 = cmdMap.get(2);
		assertEquals(1, cmd2.getAckedMsgs().get(priority).size());
		assertEquals(20, cmd2.getAckedMsgs().get(priority).get(0).getMsgSeq());
		assertEquals(1, cmd2.getNackedMsgs().get(priority).size());
		assertEquals(20, cmd2.getNackedMsgs().get(priority).get(0).getMsgSeq());
	}

	@Test
	public void testCommitCallback() throws Exception {
		when(ackManager.writeAckToBroker(any(AckMessageCommandV5.class))).thenReturn(true);

		int partition = 0;

		PulledBatch<String> batch = holder.poll(1, 10);

		final CountDownLatch latch = new CountDownLatch(1);
		final AtomicReference<Map<TopicPartition, OffsetAndMetadata>> ctx = new AtomicReference<>();
		batch.commitAsync(new OffsetCommitCallback() {

			@Override
			public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
				ctx.set(offsets);
				latch.countDown();
			}
		});
		assertTrue(latch.await(100, TimeUnit.MILLISECONDS));
		// never polled/collected, nothing to commit
		assertFalse(ctx.get().containsKey(new TopicPartition(topic, partition)));

		holder.onMessage(makeMessages(partition, false, "a", 1));
		batch = holder.poll(1, 10);

		final CountDownLatch latch2 = new CountDownLatch(1);
		final AtomicReference<Map<TopicPartition, OffsetAndMetadata>> ctx2 = new AtomicReference<>();
		batch.commitAsync(new OffsetCommitCallback() {

			@Override
			public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
				ctx2.set(offsets);
				latch2.countDown();
			}
		});
		assertTrue(latch2.await(config.getManualCommitInterval() * 2, TimeUnit.MILLISECONDS));
		assertEquals(1, ctx2.get().size());
		for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : ctx2.get().entrySet()) {
			assertEquals(new TopicPartition(topic, partition), entry.getKey());
		}

	}

	@Test
	public void testCommitSync() throws Exception {
		final CountDownLatch latch = new CountDownLatch(1);
		final long ackDelay = 200;
		when(ackManager.writeAckToBroker(any(AckMessageCommandV5.class))).thenAnswer(new Answer<Boolean>() {

			@Override
			public Boolean answer(InvocationOnMock invocation) throws Throwable {
				Thread.sleep(ackDelay);
				latch.countDown();
				return true;
			}
		});

		int partition = 0;
		holder.onMessage(makeMessages(partition, false, "a", 1));
		PulledBatch<String> batch = holder.poll(1, 10);

		long start = System.currentTimeMillis();
		batch.commitSync();
		assertTrue(System.currentTimeMillis() - start > ackDelay);
		assertTrue(latch.await(100, TimeUnit.MILLISECONDS));
	}

	@Test
	public void testCommitManyMessages() throws Exception {
		when(ackManager.writeAckToBroker(any(AckMessageCommandV5.class))).thenReturn(true);

		int msgPerPartitionPriority = 5;
		for (int i = 0; i < partitionCount; i++) {
			ArrayList<ConsumerMessage<String>> msgs = new ArrayList<>();
			long offset = i + 1;
			for (int j = 0; j < msgPerPartitionPriority; j++) {
				msgs.add(makeMessage(i, true, i + "true", offset++, false, 0));
			}
			offset = i + 11;
			for (int j = 0; j < msgPerPartitionPriority; j++) {
				msgs.add(makeMessage(i, false, i + "true", offset++, false, 0));
			}
			holder.onMessage(msgs);
		}

		final CountDownLatch latch = new CountDownLatch(1);
		final AtomicReference<Map<TopicPartition, OffsetAndMetadata>> ctx = new AtomicReference<>();
		int expectedMsgCount = msgPerPartitionPriority * 2 * partitionCount;
		PulledBatch<String> batch = holder.poll(expectedMsgCount, 1000);
		List<ConsumerMessage<String>> msgs = batch.getMessages();
		assertEquals(expectedMsgCount, msgs.size());
		batch.commitAsync(new OffsetCommitCallback() {

			@Override
			public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
				ctx.set(offsets);
				latch.countDown();
			}
		});
		assertTrue(latch.await(config.getManualCommitInterval() * 2, TimeUnit.MILLISECONDS));

		assertEquals(partitionCount, ctx.get().size());
		for (int i = 0; i < partitionCount; i++) {
			OffsetAndMetadata offsetAndMeta = ctx.get().get(new TopicPartition(topic, i));
			assertNotNull(offsetAndMeta);
			assertEquals(Long.valueOf(i + msgPerPartitionPriority), offsetAndMeta.getPriorityOffset());
			assertEquals(Long.valueOf(10 + i + msgPerPartitionPriority), offsetAndMeta.getNonPriorityOffset());
		}
	}

	@Test
	public void testNack() throws InterruptedException {
		holder.onMessage(makeMessages(0, false, "a", 1));

		final CountDownLatch latch = new CountDownLatch(1);
		final AtomicReference<AckMessageCommandV5> cmd = new AtomicReference<>();
		when(ackManager.writeAckToBroker(any(AckMessageCommandV5.class))).thenAnswer(new Answer<Boolean>() {
			@Override
			public Boolean answer(InvocationOnMock invocation) throws Throwable {
				cmd.set(invocation.getArgumentAt(0, AckMessageCommandV5.class));
				latch.countDown();
				return true;
			}
		});

		PulledBatch<String> batch = holder.poll(1, 100);
		List<ConsumerMessage<String>> msgs = batch.getMessages();
		assertEquals(1, msgs.size());
		msgs.get(0).nack();

		batch.commitSync();

		assertTrue(latch.await(config.getManualCommitInterval() * 3, TimeUnit.MILLISECONDS));
		assertEquals(1, cmd.get().getNackedMsgs().size());
	}

	@Test
	public void testNonpriority() {
		testAckNack(false, false, 0);
	}

	@Test
	public void testPriority() {
		testAckNack(true, false, 0);
	}

	@Test
	public void testResend() {
		testAckNack(false, true, 99);
	}

	@Test
	public void testMixed() throws InterruptedException {
		// nack some message
		for (int i = 0; i < 64; i++) {
			List<Integer> toNack = new ArrayList<>();
			for (int j = 0; j < 6; j++) {
				if ((i & (1 << j)) != 0) {
					toNack.add(j);
				}
			}
			testMixedInner(toNack);
		}
	}

	private void testMixedInner(List<Integer> toNack) throws InterruptedException {
		before();
		holder.onMessage(makeMessages(1, false, "m1", 10));
		holder.onMessage(makeMessages(1, true, "m2", 20));
		holder.onMessage(makeMessages(1, false, "m3", 30, true, 100));
		holder.onMessage(makeMessages(3, false, "m4", 40));
		holder.onMessage(makeMessages(3, true, "m5", 50));
		holder.onMessage(makeMessages(3, false, "m6", 60, true, 200));

		PulledBatch<String> batch = holder.poll(100, 10);
		List<ConsumerMessage<String>> msgs = batch.getMessages();
		assertEquals(6, msgs.size());
		for (int i = 0; i < 6; i++) {
			assertEquals(i <= 2 ? 1 : 3, msgs.get(i).getPartition());
			assertEquals((i + 1) * 10, msgs.get(i).getOffset());
		}

		for (int i : toNack) {
			msgs.get(i).nack();
		}

		final List<AckMessageCommandV5> cmds = new LinkedList<>();
		final CountDownLatch latch = new CountDownLatch(2);
		when(ackManager.writeAckToBroker(any(AckMessageCommandV5.class))).thenAnswer(new Answer<Boolean>() {

			@Override
			public Boolean answer(InvocationOnMock invocation) throws Throwable {
				cmds.add(invocation.getArgumentAt(0, AckMessageCommandV5.class));
				latch.countDown();
				return true;
			}
		});

		batch.commitSync();
		assertTrue(latch.await(config.getManualCommitInterval() * 3, TimeUnit.MILLISECONDS));

		assertEquals(2, cmds.size());
		HashMap<Integer, AckMessageCommandV5> cmdMap = new HashMap<>();
		for (AckMessageCommandV5 cmd : cmds) {
			cmdMap.put(cmd.getPartition(), cmd);
		}

		@SuppressWarnings("rawtypes")
		Set nackSet = new HashSet<>(toNack);

		AckMessageCommandV5 cmd = cmdMap.get(1);

		assertEquals(1, cmd.getAckedMsgs().get(1).size());
		assertEquals(10, cmd.getAckedMsgs().get(1).get(0).getMsgSeq());
		assertEquals(1, cmd.getAckedMsgs().get(0).size());
		assertEquals(20, cmd.getAckedMsgs().get(0).get(0).getMsgSeq());
		assertEquals(1, cmd.getAckedResendMsgs().get(1).size());
		assertEquals(30, cmd.getAckedResendMsgs().get(1).get(0).getMsgSeq());

		if (nackSet.contains(0)) {
			assertEquals(1, cmd.getNackedMsgs().get(1).size());
			assertEquals(10, cmd.getNackedMsgs().get(1).get(0).getMsgSeq());
		}
		if (nackSet.contains(1)) {
			assertEquals(1, cmd.getNackedMsgs().get(0).size());
			assertEquals(20, cmd.getNackedMsgs().get(0).get(0).getMsgSeq());
		}
		if (nackSet.contains(2)) {
			assertEquals(1, cmd.getNackedResendMsgs().get(1).size());
			assertEquals(30, cmd.getNackedResendMsgs().get(1).get(0).getMsgSeq());
		}

		cmd = cmdMap.get(3);

		assertEquals(1, cmd.getAckedMsgs().get(1).size());
		assertEquals(40, cmd.getAckedMsgs().get(1).get(0).getMsgSeq());
		assertEquals(1, cmd.getAckedMsgs().get(0).size());
		assertEquals(50, cmd.getAckedMsgs().get(0).get(0).getMsgSeq());
		assertEquals(1, cmd.getAckedResendMsgs().get(1).size());
		assertEquals(60, cmd.getAckedResendMsgs().get(1).get(0).getMsgSeq());

		if (nackSet.contains(3)) {
			assertEquals(1, cmd.getNackedMsgs().get(1).size());
			assertEquals(40, cmd.getNackedMsgs().get(1).get(0).getMsgSeq());
		}
		if (nackSet.contains(4)) {
			assertEquals(1, cmd.getNackedMsgs().get(0).size());
			assertEquals(50, cmd.getNackedMsgs().get(0).get(0).getMsgSeq());
		}
		if (nackSet.contains(5)) {
			assertEquals(1, cmd.getNackedResendMsgs().get(1).size());
			assertEquals(60, cmd.getNackedResendMsgs().get(1).get(0).getMsgSeq());
		}
	}

	private void testAckNack(boolean priority, boolean resend, int remainingRetries) {
		try {
			test(priority, resend, remainingRetries, false);
			test(priority, resend, remainingRetries, true);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private void test(boolean priority, boolean resend, int remainingRetries, boolean nack) throws Exception {
		before();
		int partition = 1;
		holder.onMessage(makeMessages(partition, priority, "a", 10, resend, remainingRetries));

		PulledBatch<String> batch = holder.poll(5, 10);
		List<ConsumerMessage<String>> msgs = batch.getMessages();
		assertEquals(1, msgs.size());
		assertEquals(1, msgs.get(0).getPartition());
		assertEquals(10, msgs.get(0).getOffset());
		if (nack) {
			msgs.get(0).nack();
		}

		final List<AckMessageCommandV5> cmds = new LinkedList<>();
		final CountDownLatch latch = new CountDownLatch(1);
		when(ackManager.writeAckToBroker(any(AckMessageCommandV5.class))).thenAnswer(new Answer<Boolean>() {

			@Override
			public Boolean answer(InvocationOnMock invocation) throws Throwable {
				cmds.add(invocation.getArgumentAt(0, AckMessageCommandV5.class));
				latch.countDown();
				return true;
			}
		});

		batch.commitSync();
		assertTrue(latch.await(config.getManualCommitInterval() * 3, TimeUnit.MILLISECONDS));

		assertEquals(1, cmds.size());
		HashMap<Integer, AckMessageCommandV5> cmdMap = new HashMap<>();
		for (AckMessageCommandV5 cmd : cmds) {
			cmdMap.put(cmd.getPartition(), cmd);
		}

		int intPriority = priority ? 0 : 1;
		AckMessageCommandV5 cmd1 = cmdMap.get(partition);

		Map<Integer, List<AckContext>> ackedToCheck;
		Map<Integer, List<AckContext>> nackedToCheck;

		if (resend) {
			ackedToCheck = cmd1.getAckedResendMsgs();
			nackedToCheck = cmd1.getNackedResendMsgs();
		} else {
			ackedToCheck = cmd1.getAckedMsgs();
			nackedToCheck = cmd1.getNackedMsgs();
		}

		assertEquals(1, ackedToCheck.get(intPriority).size());
		assertEquals(10, ackedToCheck.get(intPriority).get(0).getMsgSeq());
		if (nack) {
			assertEquals(1, nackedToCheck.get(intPriority).size());
			assertEquals(10, nackedToCheck.get(intPriority).get(0).getMsgSeq());
		}
	}

	private List<ConsumerMessage<String>> makeMessages(int partition, boolean priority, String body, long offset) {
		List<ConsumerMessage<String>> result = new ArrayList<>();
		ConsumerMessage<String> msg = makeMessage(partition, priority, body, offset, false, 0);
		result.add(msg);
		return result;
	}

	private List<ConsumerMessage<String>> makeMessages(int partition, boolean priority, String body, long offset,
	      boolean resend, int remainingRetries) {
		List<ConsumerMessage<String>> result = new ArrayList<>();
		ConsumerMessage<String> msg = makeMessage(partition, priority, body, offset, resend, remainingRetries);
		result.add(msg);
		return result;
	}

	private ConsumerMessage<String> makeMessage(int partition, boolean priority, String body, long offset,
	      boolean resend, int remainingRetries) {
		BaseConsumerMessage<String> baseMsg = new BaseConsumerMessage<>();
		baseMsg.setBody(body);
		baseMsg.setTopic(topic);
		baseMsg.setRemainingRetries(remainingRetries);

		BrokerConsumerMessage<String> brokerMsg = new BrokerConsumerMessage<>(baseMsg);
		brokerMsg.setGroupId(groupId);
		brokerMsg.setPartition(partition);
		brokerMsg.setPriority(priority);
		brokerMsg.setMsgSeq(offset);
		brokerMsg.setResend(resend);

		return new PullBrokerConsumerMessage<String>(brokerMsg);
	}

}
