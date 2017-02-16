package com.ctrip.hermes.metaserver.fulltest;

import org.junit.Test;

import com.ctrip.hermes.core.lease.Lease;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MetaServerBrokerLeaseTest extends MetaServerBaseTest  {

	/**
	 * Basic Broker Lease:
	 * 对于分配好的Broker: 任意时间acquire都成功
	 * 到期前 renew 成功
	 * 到期后 renew 失败
	 */
	@Test
	public void brokerLeaseBasicTest() throws Exception {
		final String topic = "meta_test_1";
		final int partition = 0;

		int brokerPort = mockBrokerRegisterToZK(1).get(0);
		final String sessionId = String.valueOf(brokerPort);

		startMultipleMetaServers(1);

		Thread.sleep(1000); //wait a while for slow CI machine.
		Lease lease = initLeaseToBroker(brokerPort, topic, 0, sessionId);
		assertNotNull(lease);
		long leaseId = lease.getId();

		// Before timeout: acquire true, renew true
		assertAcquireBrokerLeaseOnAll(true, brokerPort, topic, partition, sessionId);
		assertRenewBrokerLeaseOnAll(true, brokerPort, topic, partition, leaseId, sessionId);

		waitOneLeaseTime();
		assertTrue(lease.isExpired());

		// But still need to wait 5 second to BaseLeaseHolder.startHouseKeeper()
		Thread.sleep(5000);

		// After timeout: renew fail, acquire true, renew true;
		assertRenewBrokerLeaseOnAll(false, brokerPort, topic, partition, leaseId, sessionId);
		assertAcquireBrokerLeaseOnAll(true, brokerPort, topic, partition, sessionId);
		assertRenewBrokerLeaseOnAll(true, brokerPort, topic, partition, leaseId + 1, sessionId);

		stopMultipleMetaServersRandomly(1);
		assertAllStopped();
	}


	/**
	 * Broker Lease: 基本acquire和renew功能
	 * 1) 初始：Broker1 获得Topic1的Lease, 另有Broker2
	 * 2) 则
	 * 1) 到期前：
	 * Broker1 acquire Topic1 True
	 * Broker1 renew Topic1 True and added Lease Timeout Time!
	 * Broker2 acquire Topic1 Fail
	 * Broker2 renew Topic1 Fail
	 * 2) 到期后：
	 * Broker2 acquire Topic1 Fail
	 * Broker2 renew Topic1 Fail
	 * Broker1 acquire Topic1 True
	 * Broker1 renew Topic1 True
	 * <p/>
	 * 目标MetaServer覆盖Leader, Follower
	 */
	@Test
	public void testBrokerLeaseAcquireAndRenew() throws Exception {
		final String topic = "meta_test_1";
		final int partition = 0;
		final int metaServerCount = 2;
		// init
		startMultipleMetaServers(1);  // make localhost:1248 to be the leader
		startMultipleMetaServers(metaServerCount - 1);

		int brokerPort1 = mockBrokerRegisterToZK(1).get(0);
		String brokerSession1 = String.valueOf(brokerPort1);

		Thread.sleep(500); // for ZK discovery broke
		Lease lease = initLeaseToBroker(brokerPort1, topic, partition, brokerSession1);
		assertNotNull(lease);
		long leaseId = lease.getId();

		// Before timeout: Broker1: acquire true, renew true
		assertAcquireBrokerLeaseOnAll(true, brokerPort1, topic, partition, brokerSession1);
		assertRenewBrokerLeaseOnAll(true, brokerPort1, topic, partition, leaseId, brokerSession1);

		// Mock Broker2
		int brokerPort2 = mockBrokerRegisterToZK(1).get(0);
		String brokerSession2 = String.valueOf(brokerPort2);
		Lease lease2 = initLeaseToBroker(brokerPort2, topic, partition, brokerSession2);
		assertNull(lease2);

		// Before timeout: Broker2: acquire false, renew false
		assertAcquireBrokerLeaseOnAll(false, brokerPort2, topic, partition, brokerSession2);
		assertRenewBrokerLeaseOnAll(false, brokerPort2, topic, partition, leaseId, brokerSession2);

		// Lease has been renewed in every meta server!
		for (int i = 0; i < metaServerCount; i++) {
			waitOneLeaseTime();
		}
		assertTrue(lease.isExpired());
		// But still need to wait 5 second to BaseLeaseHolder.startHouseKeeper()
		Thread.sleep(5100);

		// After timeout: Broker1: renew fail, acquire true, renew true;
		assertRenewBrokerLeaseOnAll(false, brokerPort1, topic, partition, leaseId, brokerSession1);
		assertAcquireBrokerLeaseOnAll(true, brokerPort1, topic, partition, brokerSession1);
		assertRenewBrokerLeaseOnAll(true, brokerPort1, topic, partition, leaseId + 1, brokerSession1);

		// After timeout: Broker2: renew fail, acquire fail;
		assertRenewBrokerLeaseOnAll(false, brokerPort2, topic, partition, leaseId, brokerSession2);
		assertAcquireBrokerLeaseOnAll(false, brokerPort2, topic, partition, brokerSession2);

		// stop all servers
		stopMultipleMetaServersRandomly(metaServerCount);
		assertAllStopped();
	}
}
