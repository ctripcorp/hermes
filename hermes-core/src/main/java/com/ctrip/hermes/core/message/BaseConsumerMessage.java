package com.ctrip.hermes.core.message;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;

import com.ctrip.hermes.core.message.ConsumerMessage.MessageStatus;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class BaseConsumerMessage<T> {
	protected long m_bornTime;

	protected String m_refKey;

	protected String m_topic;

	protected T m_body;

	protected PropertiesHolder m_propertiesHolder = new PropertiesHolder();

	protected AtomicReference<MessageStatus> m_status = new AtomicReference<MessageStatus>(MessageStatus.NOT_SET);

	protected int m_remainingRetries = 0;

	protected long m_onMessageStartTimeMills;

	protected long m_onMessageEndTimeMills;

	public long getOnMessageStartTimeMills() {
		return m_onMessageStartTimeMills;
	}

	public void setOnMessageStartTimeMills(long onMessageStartTimeMills) {
		m_onMessageStartTimeMills = onMessageStartTimeMills;
	}

	public long getOnMessageEndTimeMills() {
		return m_onMessageEndTimeMills;
	}

	public void setOnMessageEndTimeMills(long onMessageEndTimeMills) {
		m_onMessageEndTimeMills = onMessageEndTimeMills;
	}

	public int getRemainingRetries() {
		return m_remainingRetries;
	}

	public void setRemainingRetries(int remainingRetries) {
		m_remainingRetries = remainingRetries;
	}

	public long getBornTime() {
		return m_bornTime;
	}

	public void setBornTime(long bornTime) {
		m_bornTime = bornTime;
	}

	public String getRefKey() {
		return m_refKey;
	}

	public void setRefKey(String key) {
		m_refKey = key;
	}

	public String getTopic() {
		return m_topic;
	}

	public void setTopic(String topic) {
		m_topic = topic;
	}

	public T getBody() {
		return m_body;
	}

	public void setBody(T body) {
		m_body = body;
	}

	public MessageStatus getStatus() {
		return m_status.get();
	}

	public boolean ack() {
		boolean setSuccess = m_status.compareAndSet(MessageStatus.NOT_SET, MessageStatus.SUCCESS);
		if (setSuccess) {
			m_onMessageEndTimeMills = System.currentTimeMillis();
		}

		return setSuccess;
	}

	public boolean nack() {
		boolean setSuccess = m_status.compareAndSet(MessageStatus.NOT_SET, MessageStatus.FAIL);
		if (setSuccess) {
			m_onMessageEndTimeMills = System.currentTimeMillis();
		}

		return setSuccess;
	}

	public void resetStatus() {
		m_status.set(MessageStatus.NOT_SET);
	}

	public void setPropertiesHolder(PropertiesHolder propertiesHolder) {
		m_propertiesHolder = propertiesHolder;
	}

	public void addDurableAppProperty(String name, String value) {
		m_propertiesHolder.addDurableAppProperty(name, value);
	}

	public void addDurableSysProperty(String name, String value) {
		m_propertiesHolder.addDurableSysProperty(name, value);
	}

	public String getDurableAppProperty(String name) {
		return m_propertiesHolder.getDurableAppProperty(name);
	}

	public String getDurableSysProperty(String name) {
		return m_propertiesHolder.getDurableSysProperty(name);
	}

	public void addVolatileProperty(String name, String value) {
		m_propertiesHolder.addVolatileProperty(name, value);
	}

	public String getVolatileProperty(String name) {
		return m_propertiesHolder.getVolatileProperty(name);
	}

	public Iterator<String> getRawDurableAppPropertyNames() {
		return m_propertiesHolder.getRawDurableAppPropertyNames().iterator();
	}

	public PropertiesHolder getPropertiesHolder() {
		return m_propertiesHolder;
	}

	public boolean isAck() {
		return m_status.get() != MessageStatus.FAIL;
	}

	@Override
	public String toString() {
		return "BaseConsumerMessage{" + "m_bornTime=" + m_bornTime + ", m_refKey='" + m_refKey + '\'' + ", m_topic='"
		      + m_topic + '\'' + ", m_body=" + m_body + ", m_propertiesHolder=" + m_propertiesHolder + ", m_status="
		      + m_status + ", m_remainingRetries=" + m_remainingRetries + '}';
	}
}
