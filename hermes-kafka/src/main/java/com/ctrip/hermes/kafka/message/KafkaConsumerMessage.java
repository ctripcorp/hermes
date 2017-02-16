package com.ctrip.hermes.kafka.message;

import java.util.Iterator;

import com.ctrip.hermes.core.message.BaseConsumerMessage;
import com.ctrip.hermes.core.message.BaseConsumerMessageAware;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.message.PropertiesHolder;
import com.ctrip.hermes.core.message.PropertiesHolderAware;

public class KafkaConsumerMessage<T> implements ConsumerMessage<T>, PropertiesHolderAware, BaseConsumerMessageAware<T> {

	private BaseConsumerMessage<T> m_baseMsg;

	private long m_msgSeq;

	private int partition;

	private long offset;

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public KafkaConsumerMessage(BaseConsumerMessage baseMsg, int partition, long offset) {
		m_baseMsg = baseMsg;
		this.partition = partition;
		this.offset = offset;
	}

	public BaseConsumerMessage<T> getBaseMsg() {
		return m_baseMsg;
	}

	public long getMsgSeq() {
		return m_msgSeq;
	}

	public void setMsgSeq(long msgSeq) {
		this.m_msgSeq = msgSeq;
	}

	@Override
	public void nack() {
		m_baseMsg.nack();
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
		m_baseMsg.ack();
	}

	@Override
	public MessageStatus getStatus() {
		return m_baseMsg.getStatus();
	}

	@Override
	public PropertiesHolder getPropertiesHolder() {
		return m_baseMsg.getPropertiesHolder();
	}

	@Override
	public BaseConsumerMessage<T> getBaseConsumerMessage() {
		return m_baseMsg;
	}

	@Override
	public int getPartition() {
		return partition;
	}

	public long getOffset() {
		return offset;
	}

	@Override
	public int getResendTimes() {
		return 0;
	}

	@Override
	public boolean isPriority() {
		return false;
	}

	@Override
	public boolean isResend() {
		return false;
	}

	@Override
	public int getRemainingRetries() {
		return 0;
	}
}
