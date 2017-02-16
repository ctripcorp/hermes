package com.ctrip.hermes.env;

import java.io.IOException;
import java.util.Properties;

public interface ClientEnvironment {

	Properties getProducerConfig(String topic) throws IOException;

	Properties getConsumerConfig(String topic) throws IOException;

	Properties getGlobalConfig();

	String getEnv();

	String getIdc();

	String getMetaServerDomainName();
}
