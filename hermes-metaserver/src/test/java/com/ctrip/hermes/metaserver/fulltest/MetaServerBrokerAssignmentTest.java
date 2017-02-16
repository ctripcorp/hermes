package com.ctrip.hermes.metaserver.fulltest;

import org.junit.Test;

public class MetaServerBrokerAssignmentTest extends MetaServerBaseTest {

	/**
	 * 增减Topic, 分配到各Broker正确。
	 */
	@Test
	public void testBrokerAssignment1() throws Exception {
		int initServers = 2;
		int brokerCount = 3;
		int topicCount = 3;

		// init
		startMultipleMetaServers(initServers);
		assertOnlyOneLeader();

		mockBrokerRegisterToZK(brokerCount);

		// add topic
		for (int i = 0; i < topicCount; i++) {
			addOneTopicToMeta();

			assertBrokerAssignmentRight();
		}

		// remove topic
		for (int i = 0; i < topicCount; i++) {
			removeOneTopicFromMeta();
			assertBrokerAssignmentRight();
		}

		mockBrokerAllUnRegisterToZK();

		// stop all servers
		stopMultipleMetaServersRandomly(initServers);
		assertAllStopped();
	}


	/**
	 * 增减Broker,分配到各Broker正确。
	 */
	@Test
	public void testBrokerAssignment2() throws Exception {
		int initServers = 2;
		int brokerCount = 3;
		int topicCount = 5;

		// init
		startMultipleMetaServers(initServers);
		assertOnlyOneLeader();

		// add topic
		for (int i = 0; i < topicCount; i++) {
			addOneTopicToMeta();
		}

		mockBrokerRegisterToZK(brokerCount);

		// broker was registered by one by one. need Time for service discovery
		Thread.sleep(brokerCount * 500);
		assertBrokerAssignmentRight();

		mockBrokerAllUnRegisterToZK();
		assertBrokerAssignmentRight();

		// remove topic
		for (int i = 0; i < topicCount; i++) {
			removeOneTopicFromMeta();
			assertBrokerAssignmentRight();
		}

		// stop all servers
		stopMultipleMetaServersRandomly(initServers);
		assertAllStopped();
	}
}
