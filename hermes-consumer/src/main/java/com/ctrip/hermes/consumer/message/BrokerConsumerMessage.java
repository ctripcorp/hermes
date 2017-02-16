package com.ctrip.hermes.consumer.message;

import io.netty.channel.Channel;

import java.util.Iterator;

import com.ctrip.hermes.consumer.engine.ack.AckManager;
import com.ctrip.hermes.core.message.BaseConsumerMessage;
import com.ctrip.hermes.core.message.BaseConsumerMessageAware;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.message.PropertiesHolder;
import com.ctrip.hermes.core.message.PropertiesHolderAware;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class BrokerConsumerMessage<T> implements ConsumerMessage<T>, PropertiesHolderAware, BaseConsumerMessageAware<T> {

	private BaseConsumerMessage<T> m_baseMsg;

	private long m_msgSeq;

	private int m_partition;

	private boolean m_priority;

	private boolean m_resend = false;

	private String m_groupId;

	private long m_token;

	private Channel m_channel;

	private int m_retryTimesOfRetryPolicy;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public BrokerConsumerMessage(BaseConsumerMessage baseMsg) {
		m_baseMsg = baseMsg;
	}

	public String getGroupId() {
		return m_groupId;
	}

	public void setGroupId(String groupId) {
		m_groupId = groupId;
	}

	public long getToken() {
		return m_token;
	}

	public void setToken(long token) {
		m_token = token;
	}

	public void setChannel(Channel channel) {
		m_channel = channel;
	}

	@Override
	public boolean isPriority() {
		return m_priority;
	}

	public void setPriority(boolean priority) {
		m_priority = priority;
	}

	public int getPartition() {
		return m_partition;
	}

	public void setPartition(int partition) {
		m_partition = partition;
	}

	public long getMsgSeq() {
		return m_msgSeq;
	}

	public void setMsgSeq(long msgSeq) {
		this.m_msgSeq = msgSeq;
	}

	@Override
	public void nack() {
		if (m_baseMsg.nack()) {
			PlexusComponentLocator.lookup(AckManager.class).nack(m_token, this);
		}
	}

	@Override
	public String getProperty(String name) {
		return m_baseMsg.getDurableAppProperty(name);
	}

	@Override
	public Iterator<String> getPropertyNames() {
		return m_baseMsg.getRawDurableAppPropertyNames();
	}

	@Override
	public long getBornTime() {
		return m_baseMsg.getBornTime();
	}

	@Override
	public String getTopic() {
		return m_baseMsg.getTopic();
	}

	@Override
	public String getRefKey() {
		return m_baseMsg.getRefKey();
	}

	@Override
	public T getBody() {
		return m_baseMsg.getBody();
	}

	@Override
	public void ack() {
		if (m_baseMsg.ack()) {
			PlexusComponentLocator.lookup(AckManager.class).ack(m_token, this);
		}
	}

	@Override
	public MessageStatus getStatus() {
		return m_baseMsg.getStatus();
	}

	public void setResend(boolean resend) {
		m_resend = resend;
	}

	@Override
	public boolean isResend() {
		return m_resend;
	}

	public int getRemainingRetries() {
		return m_baseMsg.getRemainingRetries();
	}

	public PropertiesHolder getPropertiesHolder() {
		return m_baseMsg.getPropertiesHolder();
	}

	@Override
	public String toString() {
		return "BrokerConsumerMessage{" + "m_baseMsg=" + m_baseMsg + ", m_msgSeq=" + m_msgSeq + ", m_partition="
		      + m_partition + ", m_priority=" + m_priority + ", m_resend=" + m_resend + ", m_groupId='" + m_groupId
		      + '\'' + ", token=" + m_token + ", m_channel=" + m_channel + '}';
	}

	@Override
	public BaseConsumerMessage<T> getBaseConsumerMessage() {
		return m_baseMsg;
	}

	public long getOffset() {
		return this.getMsgSeq();
	}

	public int getRetryTimesOfRetryPolicy() {
		return m_retryTimesOfRetryPolicy;
	}

	public void setRetryTimesOfRetryPolicy(int retryTimesOfRetryPolicy) {
		m_retryTimesOfRetryPolicy = retryTimesOfRetryPolicy;
	}

	@Override
	public int getResendTimes() {
		if (isResend()) {
			return m_retryTimesOfRetryPolicy - m_baseMsg.getRemainingRetries() + 1;
		} else {
			return 0;
		}
	}
}
