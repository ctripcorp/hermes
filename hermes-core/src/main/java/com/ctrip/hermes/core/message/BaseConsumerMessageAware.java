package com.ctrip.hermes.core.message;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface BaseConsumerMessageAware<T> {
	public BaseConsumerMessage<T> getBaseConsumerMessage();
}
