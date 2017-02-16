package com.ctrip.hermes.consumer.integration;

import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.ctrip.hermes.consumer.DefaultConsumer.DefaultConsumerHolder;
import com.ctrip.hermes.consumer.api.Consumer;
import com.ctrip.hermes.consumer.api.Consumer.ConsumerHolder;
import com.ctrip.hermes.consumer.engine.notifier.ConsumerNotifier;
import com.ctrip.hermes.consumer.integration.assist.LeaseAnswer;
import com.ctrip.hermes.consumer.integration.assist.PullMessageAnswer;
import com.ctrip.hermes.consumer.integration.assist.RawMessageCreator;
import com.ctrip.hermes.consumer.integration.assist.TestMessageListener;
import com.ctrip.hermes.consumer.integration.assist.TestObjectMessage;
import com.ctrip.hermes.core.transport.command.CorrelationIdGenerator;
import com.ctrip.hermes.meta.entity.Meta;

@RunWith(MockitoJUnitRunner.class)
public class ConsumerIntegrationTest extends BaseConsumerIntegrationTest {

	private static final TestObjectMessage TEST_MSG = new TestObjectMessage("test", 123, new byte[] { 0, 1, 2, 3 });

	private static final RawMessageCreator<TestObjectMessage> SIMPLE_CREATOR = new RawMessageCreator<TestObjectMessage>() {
		@Override
		public List<List<TestObjectMessage>> createRawMessages() {
			return Arrays.asList(Arrays.asList(TEST_MSG));
		}
	};

	@Override
	public void setUp() throws Exception {
		super.setUp();
		metaProxyActions4LeaseOperation(LeaseAnswer.SUCCESS, LeaseAnswer.SUCCESS);
	}

	private void waitUntilConsumerStarted(ConsumerHolder holder) {
		while (!((DefaultConsumerHolder) holder).isConsuming()) {
			try {
				TimeUnit.MILLISECONDS.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	@Test
	public void testBasicConsume() throws Exception {
		brokerActions4PollMessageCmd(PullMessageAnswer.BASIC.channel(m_channel).creator(SIMPLE_CREATOR));

		TestMessageListener msgListener = new TestMessageListener().receiveCount(1);
		ConsumerHolder consumer = Consumer.getInstance().start(TEST_TOPIC, TEST_GROUP, msgListener);

		msgListener.waitUntilReceivedAllMessage();

		consumer.close();

		Assert.assertFalse(!((DefaultConsumerHolder) consumer).isConsuming());
		Assert.assertEquals(1, msgListener.getReceivedMessages().size());
		Assert.assertEquals(TEST_MSG, msgListener.getReceivedMessages().get(0));

		msgListener.countDownAll();
	}

	@Test
	public void testBasicMultipleMessageConsume() throws Exception {
		final int batchCount = 3;
		final int msgCountPerBatch = 5;
		brokerActions4PollMessageCmd(PullMessageAnswer.BASIC.channel(m_channel).creator(
		      new RawMessageCreator<TestObjectMessage>() {
			      @Override
			      public List<List<TestObjectMessage>> createRawMessages() {
				      List<List<TestObjectMessage>> batchs = new ArrayList<>();
				      for (int i = 0; i < batchCount; i++) {
					      List<TestObjectMessage> msgs = new ArrayList<>();
					      for (int j = 0; j < msgCountPerBatch; j++) {
						      msgs.add(new TestObjectMessage("hermes.msg." + (i * msgCountPerBatch + j), i * msgCountPerBatch
						            + j, new byte[] { 17 }));
					      }
					      batchs.add(msgs);
				      }
				      return batchs;
			      }
		      }));

		TestMessageListener msgListener = new TestMessageListener().receiveCount(batchCount * msgCountPerBatch);
		ConsumerHolder consumer = Consumer.getInstance().start(TEST_TOPIC, TEST_GROUP, msgListener);

		msgListener.waitUntilReceivedAllMessage();

		consumer.close();
		System.out.println(msgListener.getReceivedMessages());

		Assert.assertFalse(!((DefaultConsumerHolder) consumer).isConsuming());
		Assert.assertEquals(batchCount * msgCountPerBatch, msgListener.getReceivedMessages().size());
		for (int i = 0; i < batchCount; i++) {
			for (int j = 0; j < msgCountPerBatch; j++) {
				Assert.assertEquals(17,
				      msgListener.getReceivedMessages().get(i * msgCountPerBatch + j).getByteArrayValue()[0]);
			}
		}
		msgListener.countDownAll();
	}

	@Test
	public void testPullMessageTimeout() throws Exception {
		brokerActions4PollMessageCmd(PullMessageAnswer.BASIC.withDelay(300).channel(m_channel).creator(SIMPLE_CREATOR));

		TestMessageListener msgListener = new TestMessageListener().receiveCount(1);
		ConsumerHolder consumer = Consumer.getInstance().start(TEST_TOPIC, TEST_GROUP, msgListener);

		msgListener.waitUntilReceivedAllMessage();

		consumer.close();

		Assert.assertFalse(!((DefaultConsumerHolder) consumer).isConsuming());
		Assert.assertEquals(1, msgListener.getReceivedMessages().size());
		Assert.assertEquals(TEST_MSG, msgListener.getReceivedMessages().get(0));
		msgListener.countDownAll();
	}

	@Test
	public void testConsumeFailed() {
		brokerActions4PollMessageCmd(PullMessageAnswer.BASIC.channel(m_channel).creator(SIMPLE_CREATOR));

		TestMessageListener msgListener = new TestMessageListener().receiveCount(1).withError(true);
		ConsumerHolder consumer = Consumer.getInstance().start(TEST_TOPIC, TEST_GROUP, msgListener);

		msgListener.waitUntilReceivedAllMessage();

		consumer.close();

		Assert.assertEquals(1, msgListener.getReceivedMessages().size());
		Assert.assertEquals(TEST_MSG, msgListener.getReceivedMessages().get(0));
		msgListener.countDownAll();
	}

	@Test
	public void testNoResponseWhenPullMessage() throws Exception {
		brokerActions4PollMessageCmd(PullMessageAnswer.NO_ANSWER);

		TestMessageListener msgListener = new TestMessageListener().receiveCount(1);
		ConsumerHolder consumer = Consumer.getInstance().start(TEST_TOPIC, TEST_GROUP, msgListener);

		waitUntilConsumerStarted(consumer);
		msgListener.waitUntilReceivedAllMessage(100);

		consumer.close();

		Assert.assertEquals(0, msgListener.getReceivedMessages().size());
		msgListener.countDownAll();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testTopicNotAllowed() {
		Consumer.getInstance().start("non.exist.topic", TEST_GROUP, new TestMessageListener());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testGroupNotAllowed() {
		ConsumerHolder holder = Consumer.getInstance().start(TEST_TOPIC, "non.exist.group", new TestMessageListener());
		Assert.assertFalse(((DefaultConsumerHolder) holder).isConsuming());
		holder.close();
	}

	@Test
	public void testAcquireLeaseFailed() throws Exception {
		metaProxyActions4LeaseOperation(LeaseAnswer.FAILED, LeaseAnswer.FAILED);
		doTestConsumeFailed();
	}

	@Test
	public void testAcquireLeaseFailedWithNull() throws Exception {
		metaProxyActions4LeaseOperation(LeaseAnswer.FAILED_NULL, LeaseAnswer.FAILED_NULL);
		doTestConsumeFailed();
	}

	@Test
	public void testAcquireLeaseTimeout() throws Exception {
		metaProxyActions4LeaseOperation(LeaseAnswer.SUCCESS.withDelay(200), LeaseAnswer.SUCCESS);
		doTestConsumeFailed();
	}

	private void doTestConsumeFailed() throws Exception {
		brokerActions4PollMessageCmd(PullMessageAnswer.BASIC.channel(m_channel).creator(SIMPLE_CREATOR));
		TestMessageListener listener = new TestMessageListener().receiveCount(1);
		ConsumerHolder holder = Consumer.getInstance().start(TEST_TOPIC, TEST_GROUP, listener);
		waitUntilConsumerStarted(holder);
		listener.waitUntilReceivedAllMessage(300);
		holder.close();
		Assert.assertEquals(0, listener.getReceivedMessages().size());
		listener.countDownAll();
	}

	@Test
	public void testRenewLeaseFailed() throws Exception {
		long id = CorrelationIdGenerator.generateCorrelationId() + 1;

		ConsumerNotifier notifier = lookup(ConsumerNotifier.class);

		metaProxyActions4LeaseOperation(LeaseAnswer.SUCCESS, LeaseAnswer.SUCCESS);
		brokerActions4PollMessageCmd(PullMessageAnswer.BASIC.channel(m_channel).creator(SIMPLE_CREATOR));

		TestMessageListener listener = new TestMessageListener().receiveCount(1);
		ConsumerHolder holder = Consumer.getInstance().start(TEST_TOPIC, TEST_GROUP, listener);
		waitUntilConsumerStarted(holder);
		listener.waitUntilReceivedAllMessage();
		Assert.assertEquals(1, listener.getReceivedMessages().size());
		holder.close();
		listener.countDownAll();

		metaProxyActions4LeaseOperation(LeaseAnswer.SUCCESS, LeaseAnswer.FAILED);
		brokerActions4PollMessageCmd(PullMessageAnswer.BASIC.channel(m_channel).creator(SIMPLE_CREATOR));

		listener = new TestMessageListener().receiveCount(1);
		ConsumerHolder holder_failed = Consumer.getInstance().start(TEST_TOPIC, TEST_GROUP, listener);
		waitUntilConsumerStarted(holder_failed);
		listener.waitUntilReceivedAllMessage();
		Assert.assertNull(notifier.find(id));
		holder_failed.close();
		listener.countDownAll();
	}

	@Test
	public void testGetEndpointFailed() throws Exception {
		brokerActions4PollMessageCmd(PullMessageAnswer.BASIC.channel(m_channel).creator(SIMPLE_CREATOR));
		TestMessageListener listener = new TestMessageListener().receiveCount(1);
		ConsumerHolder holder = Consumer.getInstance().start(TEST_TOPIC, TEST_GROUP, listener);
		long preCost = listener.waitUntilReceivedAllMessage();
		Assert.assertEquals(1, listener.getReceivedMessages().size());
		holder.close();
		listener.countDownAll();

		Meta meta = loadLocalMeta();
		meta.getEndpoints().clear();
		when(m_metaHolder.getMeta()).thenReturn(meta);

		brokerActions4PollMessageCmd(PullMessageAnswer.BASIC.channel(m_channel).creator(SIMPLE_CREATOR));
		listener = new TestMessageListener().receiveCount(1);
		holder = Consumer.getInstance().start(TEST_TOPIC, TEST_GROUP, listener);
		listener.waitUntilReceivedAllMessage(preCost * 2);
		Assert.assertEquals(0, listener.getReceivedMessages().size());
		holder.close();
		listener.countDownAll();
	}
}
