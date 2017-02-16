package com.ctrip.hermes.consumer.message;

import io.netty.channel.Channel;

import java.util.Iterator;

import com.ctrip.hermes.core.message.BaseConsumerMessage;
import com.ctrip.hermes.core.message.BaseConsumerMessageAware;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.message.PropertiesHolder;
import com.ctrip.hermes.core.message.PropertiesHolderAware;

public class NackDelayedBrokerConsumerMessage<T> implements ConsumerMessage<T>, PropertiesHolderAware,
      BaseConsumerMessageAware<T> {

	private BrokerConsumerMessage<T> m_brokerMsg;

	private int m_resendTimes;

	private boolean m_resend;

	private int m_remainingRetries;

	public NackDelayedBrokerConsumerMessage(BrokerConsumerMessage<T> brokerMsg) {
		m_brokerMsg = brokerMsg;
	}

	public void nack() {
		m_resendTimes++;
		m_brokerMsg.getBaseConsumerMessage().nack();
	}

	public void setResend(boolean resend) {
		m_resend = resend;
	}

	@Override
	public int getResendTimes() {
		return m_resendTimes;
	}

	@Override
	public boolean isResend() {
		return m_resend;
	}

	public void setRemainingRetries(int remainingRetries) {
		m_remainingRetries = remainingRetries;
	}

	@Override
	public int getRemainingRetries() {
		return m_remainingRetries;
	}

	// below are delegate methods

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
		m_brokerMsg.ack();
	}

	public com.ctrip.hermes.core.message.ConsumerMessage.MessageStatus getStatus() {
		return m_brokerMsg.getStatus();
	}

	public PropertiesHolder getPropertiesHolder() {
		return m_brokerMsg.getPropertiesHolder();
	}

	public BaseConsumerMessage<T> getBaseConsumerMessage() {
		return m_brokerMsg.getBaseConsumerMessage();
	}

	public long getOffset() {
		return m_brokerMsg.getOffset();
	}

	public int getRetryTimesOfRetryPolicy() {
		return m_brokerMsg.getRetryTimesOfRetryPolicy();
	}

	public void setRetryTimesOfRetryPolicy(int retryTimesOfRetryPolicy) {
		m_brokerMsg.setRetryTimesOfRetryPolicy(retryTimesOfRetryPolicy);
	}

}
