package com.ctrip.hermes.core.message;

import java.util.Iterator;

public interface ConsumerMessage<T> {

	public static enum MessageStatus {
		SUCCESS, FAIL, NOT_SET;
	}

	public void nack();

	public String getProperty(String name);

	public Iterator<String> getPropertyNames();

	public long getBornTime();

	public String getTopic();

	public String getRefKey();

	public T getBody();

	public MessageStatus getStatus();

	public void ack();

	public int getPartition();

	public long getOffset();

	public boolean isPriority();

	public int getResendTimes();

	public boolean isResend();

	public int getRemainingRetries();
}
