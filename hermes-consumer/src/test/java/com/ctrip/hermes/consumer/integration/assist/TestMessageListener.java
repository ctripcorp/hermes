package com.ctrip.hermes.consumer.integration.assist;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.ctrip.hermes.consumer.api.BaseMessageListener;
import com.ctrip.hermes.core.message.ConsumerMessage;

public class TestMessageListener extends BaseMessageListener<TestObjectMessage> {
	private CountDownLatch m_receiveLatch = null;

	private List<TestObjectMessage> m_receivedMessage = new ArrayList<>();

	private boolean m_throwException = false;

	public CountDownLatch getLatch() {
		return m_receiveLatch;
	}

	public TestMessageListener receiveCount(int count) {
		m_receiveLatch = new CountDownLatch(count);
		System.out.println(">>>>>>>>>>>> Listener latch: " + getLatch().getCount());
		return this;
	}

	public TestMessageListener withError(boolean shouldFail) {
		m_throwException = shouldFail;
		return this;
	}

	public boolean waitUntilReceivedAllMessage(long timeoutInMillisecond) {
		try {
			boolean success = m_receiveLatch.await(timeoutInMillisecond, TimeUnit.MILLISECONDS);
			System.out.println(">>>>>>>>>>>> Listener latch after wait: " + getLatch().getCount());
			return success;
		} catch (InterruptedException e) {
			System.out.println("!!!!!!!!!!!!!! Interrupted before receive all !!!!!!!");
			e.printStackTrace();
			return false;
		}
	}

	public long waitUntilReceivedAllMessage() {
		try {
			long begin = System.currentTimeMillis();
			m_receiveLatch.await();
			System.out.println(">>>>>>>>>>>> Listener latch after wait: " + getLatch().getCount());
			return System.currentTimeMillis() - begin;
		} catch (InterruptedException e) {
			System.out.println("!!!!!!!!!!!!!! Interrupted before receive all !!!!!!!");
			e.printStackTrace();
			return -1;
		}
	}

	public void countDownAll() {
		if (m_receiveLatch != null && m_receiveLatch.getCount() > 0) {
			for (int idx = 0; idx < m_receiveLatch.getCount();) {
				m_receiveLatch.countDown();
			}
			System.out.println(">>>>>>>>>>>> Count down all manually...");
		}
	}

	public List<TestObjectMessage> getReceivedMessages() {
		return m_receivedMessage;
	}

	@Override
	protected synchronized void onMessage(ConsumerMessage<TestObjectMessage> msg) {
		boolean shouldCountDown = false;
		if (m_receiveLatch != null) {
			if (m_receiveLatch.getCount() == 0) {
				return;
			}
			shouldCountDown = true;
		}
		try {
			handleMessage(msg);
		} finally {
			if (shouldCountDown) {
				m_receiveLatch.countDown();
			}
		}
	}

	private void handleMessage(ConsumerMessage<TestObjectMessage> msg) {
		m_receivedMessage.add(msg.getBody());
		if (m_throwException) {
			throw new RuntimeException(">>>>> You tell me that I should throw an exception, so I did it.");
		}
	}
}
