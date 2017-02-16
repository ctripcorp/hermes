package com.ctrip.hermes.metaserver.fulltest;

import org.junit.Test;

import com.ctrip.hermes.meta.entity.Topic;

public class MetaServerBaseMetaChangeTest extends MetaServerBaseTest  {
	/**
	 *  BaseMeta/Broker/MetaServer Changes on one Topic
	 */
	@Test
	public void testBaseMetaChanged() throws Exception {
		int initServers = 3;

		// init
		startMultipleMetaServers(initServers);
		assertOnlyOneLeader();

		// baseMeta changed
		Topic topic = TopicHelper.buildRandomTopic();

		baseMetaCreateTopic(topic);
		Thread.sleep(2500); //wait metaServers notice the ZK change

		assertAllMetaServerHaveTopic(topic);

		topic = TopicHelper.buildUpdatedTopic(topic);
		baseMetaUpdateTopic(topic);
		Thread.sleep(2500); //wait metaServers notice the ZK change
		assertAllMetaServerHaveTopic(topic);

		baseMetaDeleteTopic(topic);

		Thread.sleep(2500); //wait metaServers notice the ZK change
		assertAllMetaServerHaveNotTopic(topic);

		// stop all servers
		stopMultipleMetaServersRandomly(initServers);
		assertAllStopped();
	}


}
