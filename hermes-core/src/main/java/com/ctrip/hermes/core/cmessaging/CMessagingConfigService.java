package com.ctrip.hermes.core.cmessaging;

public interface CMessagingConfigService {
	int CONSUME_FROM_CMESSAGING = 1;

	int CONSUME_FROM_HERMES = 2;

	int CONSUME_FROM_BOTH = 3;

	String getTopic(String exchangeName);

	String getGroupId(String exchangeName, String idAndQueueName);

	boolean isHermesProducerEnabled(String exchangeName, String identifier, String ip);

	int getConsumerType(String exchangeName, String identifier, String ip);
}
