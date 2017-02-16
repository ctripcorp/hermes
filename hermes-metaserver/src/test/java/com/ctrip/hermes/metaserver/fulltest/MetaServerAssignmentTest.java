package com.ctrip.hermes.metaserver.fulltest;

import org.junit.Test;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.service.ZookeeperService;

public class MetaServerAssignmentTest extends MetaServerBaseTest  {
	/**
	 * MetaServer新增，减少: 默认将Topic-Group分配到各MetaServer上
	 */
	@Test
	public void testMetaServerAssignment() throws Exception {
		int initServers = 2;
		int topicCount = 5;

		// init
		startMultipleMetaServers(initServers);
		assertOnlyOneLeader();

		// add topic
		for (int i = 0; i < topicCount; i++) {
			addOneTopicToMeta();
			assertMetaServerAssignmentRight();
		}

		// remove topic
		for (int i = 0; i < topicCount; i++) {
			Topic removedTopic = removeOneTopicFromMeta();

			// mock delete this topic on Portal
			if (null != removedTopic) {
				PlexusComponentLocator.lookup(ZookeeperService.class).
						  deleteMetaServerAssignmentZkPath(removedTopic.getName());
				assertMetaServerAssignmentRight();
			}
		}

		// stop all servers
		stopMultipleMetaServersRandomly(initServers);
		assertAllStopped();
	}

}
