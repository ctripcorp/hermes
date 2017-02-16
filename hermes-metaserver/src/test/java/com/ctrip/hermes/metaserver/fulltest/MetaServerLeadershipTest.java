package com.ctrip.hermes.metaserver.fulltest;

import org.junit.Test;

public class MetaServerLeadershipTest extends MetaServerBaseTest {

	/**
	 * 0. Basic Functions
	 */
	@Test
	public void startAndStopMultipleMetaServerAtOneTime() throws Exception {

		startMultipleMetaServers(5);

		Thread.sleep(3000);
		stopMultipleMetaServersRandomly(4);
		stopMultipleMetaServersRandomly(1);

		assertAllStopped();
	}

	/**
	 * 1. Leader Election:
	 * 多个MetaServer,时起时停,保证有且仅有一个Leader
	 */
	@Test
	public void testOnlyOneLeaderAllTime() throws Exception {
		int totalServers = 3;
		int initServers = 1;

		// init
		startMultipleMetaServers(initServers);
		assertOnlyOneLeader();

		// add one MetaServer one time.
		for (int i = 0; i < (totalServers - initServers); i++) {
			startMultipleMetaServers(1);
			assertOnlyOneLeader();
		}

		// stop one MetaServer one time.
		for (int i = totalServers; i > 0; i--) {
			stopMultipleMetaServersRandomly(1);
			if (i > 1) {
				assertOnlyOneLeader();
			}
		}

		assertAllStopped();
	}
}
