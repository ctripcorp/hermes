package com.ctrip.hermes.consumer.pull;

import io.netty.channel.Channel;

import java.util.Iterator;

import com.ctrip.hermes.consumer.message.BrokerConsumerMessage;
import com.ctrip.hermes.core.message.BaseConsumerMessage;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.message.PropertiesHolder;

public class PullBrokerConsumerMessage<T> implements ConsumerMessage<T> {

	private BrokerConsumerMessage<T> m_brokerMsg;

	private RetriveSnapshot<T> m_snapshot;

	public PullBrokerConsumerMessage(BrokerConsumerMessage<T> brokerMsg) {
		m_brokerMsg = brokerMsg;
	}

	public void setOwningSnapshot(RetriveSnapshot<T> snapshot) {
		m_snapshot = snapshot;
	}

	public String getGroupId() {
		return m_brokerMsg.getGroupId();
	}

	public void setGroupId(String groupId) {
		m_brokerMsg.setGroupId(groupId);
	}

	public long getToken() {
		return m_brokerMsg.getToken();
	}

	public void setToken(long token) {
		m_brokerMsg.setToken(token);
	}

	public void setChannel(Channel channel) {
		m_brokerMsg.setChannel(channel);
	}

	public boolean isPriority() {
		return m_brokerMsg.isPriority();
	}

	public void setPriority(boolean priority) {
		m_brokerMsg.setPriority(priority);
	}

	public int hashCode() {
		return m_brokerMsg.hashCode();
	}

	public int getPartition() {
		return m_brokerMsg.getPartition();
	}

	public void setPartition(int partition) {
		m_brokerMsg.setPartition(partition);
	}

	public long getMsgSeq() {
		return m_brokerMsg.getMsgSeq();
	}

	public void setMsgSeq(long msgSeq) {
		m_brokerMsg.setMsgSeq(msgSeq);
	}

	public void nack() {
		if (m_brokerMsg.getBaseConsumerMessage().nack()) {
			m_snapshot.addNackMessage(this);
		}
	}

	public String getProperty(String name) {
		return m_brokerMsg.getProperty(name);
	}

	public Iterator<String> getPropertyNames() {
		return m_brokerMsg.getPropertyNames();
	}

	public long getBornTime() {
		return m_brokerMsg.getBornTime();
	}

	public String getTopic() {
		return m_brokerMsg.getTopic();
	}

	public String getRefKey() {
		return m_brokerMsg.getRefKey();
	}

	public T getBody() {
		return m_brokerMsg.getBody();
	}

	public void ack() {
		// noop
	}

	public com.ctrip.hermes.core.message.ConsumerMessage.MessageStatus getStatus() {
		return m_brokerMsg.getStatus();
	}

	public void setResend(boolean resend) {
		m_brokerMsg.setResend(resend);
	}

	public boolean isResend() {
		return m_brokerMsg.isResend();
	}

	public int getRemainingRetries() {
		return m_brokerMsg.getRemainingRetries();
	}

	public PropertiesHolder getPropertiesHolder() {
		return m_brokerMsg.getPropertiesHolder();
	}

	public String toString() {
		return m_brokerMsg.toString();
	}

	public BaseConsumerMessage<T> getBaseConsumerMessage() {
		return m_brokerMsg.getBaseConsumerMessage();
	}

	public long getOffset() {
		return m_brokerMsg.getOffset();
	}

	public boolean equals(Object obj) {
		return m_brokerMsg.equals(obj);
	}

	public int getRetryTimesOfRetryPolicy() {
		return m_brokerMsg.getRetryTimesOfRetryPolicy();
	}

	public void setRetryTimesOfRetryPolicy(int retryTimesOfRetryPolicy) {
		m_brokerMsg.setRetryTimesOfRetryPolicy(retryTimesOfRetryPolicy);
	}

	public int getResendTimes() {
		return m_brokerMsg.getResendTimes();
	}

}
