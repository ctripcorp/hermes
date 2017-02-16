package com.ctrip.hermes.metaserver.fulltest;

import org.junit.Test;

/**
 * Pass: mvn test -DTest=com.ctrip.hermes.metaserver.fulltest.MetaServerConsumerLeaseChangeTest
 * Fail: Travis CI.
 */
public class MetaServerConsumerLeaseChangeTest extends MetaServerBaseTest{
	/**
	 * Test: Meta Server 变化的情况下，仍能正确拿到Lease
	 * 模拟场景：Leader 1248, Follower 1249 变成 Leader 1249, Follower 1248, 仍正确Lease。
	 */
	@Test
	public void testOrderConsumerChange() throws Exception {
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
		Thread.sleep(2000);

		assertAcquireConsumerLeaseOnAll(true, topic, group);
		assertRenewConsumerLeaseOnAll(true, 1, topic, group);

		stopMetaServer(1248);
		startMetaServer(1248);
		Thread.sleep(2000);

		assertAcquireConsumerLeaseOnAll(true, topic, group);
		assertRenewConsumerLeaseOnAll(true, 1, topic, group);

	}

	@Test
	public void testNonOrderConsumerChange() throws Exception {
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

		assertAcquireConsumerLeaseOnAll(true, topic, group);
		assertRenewConsumerLeaseOnAll(true, 1, topic, group);

		stopMetaServer(1248);
		startMetaServer(1248);
		Thread.sleep(2000);

		assertAcquireConsumerLeaseOnAll(true, topic, group);
		assertRenewConsumerLeaseOnAll(true, 1, topic, group);
	}
}
