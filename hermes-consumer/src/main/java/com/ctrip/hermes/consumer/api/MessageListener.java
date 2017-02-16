package com.ctrip.hermes.consumer.api;

import java.util.List;

import com.ctrip.hermes.core.message.ConsumerMessage;

public interface MessageListener<T> {

	public void onMessage(List<ConsumerMessage<T>> msgs);

}
