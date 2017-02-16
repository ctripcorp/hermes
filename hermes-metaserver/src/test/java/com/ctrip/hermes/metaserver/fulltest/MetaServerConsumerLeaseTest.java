package com.ctrip.hermes.metaserver.fulltest;

import org.junit.Test;

public class MetaServerConsumerLeaseTest extends MetaServerBaseTest {

	/**
	 * Consumer Lease: Basic
	 */
	@Test
	public void testOrderConsumerAcquire() throws Exception {
		final String topic = "meta_test_1";
		final String group = "meta_test_order_group";
		final int metaServerCount = 2;

		// init
		startMultipleMetaServers(1);  // make localhost:1248 to be the leader
		startMultipleMetaServers(metaServerCount - 1);

		// mock one broker
		int brokerPort = mockBrokerRegisterToZK(1).get(0);
		final String sessionId = String.valueOf(brokerPort);


		initLeaseToBroker(brokerPort, topic, 0, sessionId);

		assertAcquireConsumerLeaseOnAll(true, topic, group);
		assertRenewConsumerLeaseOnAll(true, 1, topic, group);
	}

	@Test
	public void testNonOrderConsumerAcquire() throws Exception {
		final String topic = "meta_test_1";
		final String group = "meta_test_non_order_group";
		final int metaServerCount = 2;

		// init
		startMultipleMetaServers(1);  // make localhost:1248 to be the leader
		startMultipleMetaServers(metaServerCount - 1);

		// mock one broker
		int brokerPort = mockBrokerRegisterToZK(1).get(0);
		final String sessionId = String.valueOf(brokerPort);


		initLeaseToBroker(brokerPort, topic, 0, sessionId);

		Thread.sleep(1000);
		assertAcquireConsumerLeaseOnAll(true, topic, group);
		assertRenewConsumerLeaseOnAll(true, 1, topic, group);
	}
}
