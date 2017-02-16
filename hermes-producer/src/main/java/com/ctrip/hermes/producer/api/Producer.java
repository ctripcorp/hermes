package com.ctrip.hermes.producer.api;

import java.util.concurrent.Future;

import com.ctrip.hermes.core.exception.MessageSendException;
import com.ctrip.hermes.core.result.CompletionCallback;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;

public abstract class Producer {

	public abstract MessageHolder message(String topic, String partitionKey, Object body);

	public static Producer getInstance() {
		return PlexusComponentLocator.lookup(Producer.class);
	}

	public interface MessageHolder {
		public MessageHolder withRefKey(String key);

		public Future<SendResult> send();
		
		public SendResult sendSync() throws MessageSendException;

		public MessageHolder withPriority();

		public MessageHolder addProperty(String key, String value);

		public MessageHolder setCallback(CompletionCallback<SendResult> callback);
	}

}
