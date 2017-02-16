package com.ctrip.hermes.core.env;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Properties;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.env.ClientEnvironment;

public class DefaultClientEnvironmentTest extends ComponentTestCase {

	@Test
	public void test() throws Exception {
		ClientEnvironment env = lookup(ClientEnvironment.class);
		Properties config = env.getProducerConfig("test_topic");
		assertNotNull(config);
		assertEquals(config.getProperty("hermes"), "mq");
		assertEquals(config.getProperty("inherited"), "got");

		config = env.getProducerConfig("not_exist");
		assertNotNull(config);
		assertEquals(config.getProperty("hermes"), "default");
		assertEquals(config.getProperty("inherited"), "got");
	}

}
