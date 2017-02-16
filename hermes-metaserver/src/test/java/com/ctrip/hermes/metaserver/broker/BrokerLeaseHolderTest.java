package com.ctrip.hermes.metaserver.broker;

import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class BrokerLeaseHolderTest {// extends ZKSuppportTestCase {

	// private BrokerLeaseHolder m_leaseHolder;
	//
	// public static class TestBrokerLeaaseHolder extends BrokerLeaseHolder {
	//
	// }
	//
	// private void addLeasesToZk(String topic, int partition, List<Pair<String, ClientLeaseInfo>> data) throws Exception {
	// String path = ZKPathUtils.getBrokerLeaseZkPath(topic, partition);
	// String parentPath = ZKPathUtils.getBrokerLeaseTopicParentZkPath(topic);
	//
	// byte[] bytes = null;
	//
	// if (data != null && !data.isEmpty()) {
	// Map<String, ClientLeaseInfo> leases = new HashMap<>();
	// for (Pair<String, ClientLeaseInfo> pair : data) {
	// leases.put(pair.getKey(), pair.getValue());
	// }
	// bytes = ZKSerializeUtils.serialize(leases);
	// } else {
	// bytes = new byte[0];
	// }
	//
	// ensurePath(parentPath);
	// ensurePath(path);
	//
	// m_curator.setData().forPath(path, bytes);
	// m_curator.setData().forPath(parentPath, ZKSerializeUtils.serialize(System.currentTimeMillis()));
	// }
	//
	// private void assertLeases(Map<Pair<String, Integer>, Map<String, ClientLeaseInfo>> allValidLeases, String topic,
	// int partition, List<Pair<String, ClientLeaseInfo>> expectedLeaseInfos) {
	// Pair<String, Integer> tp = new Pair<String, Integer>(topic, partition);
	// Map<String, ClientLeaseInfo> actualLeaseInfos = allValidLeases.get(tp);
	// assertEquals(expectedLeaseInfos.size(), actualLeaseInfos.size());
	//
	// for (Pair<String, ClientLeaseInfo> expectedLeaseInfo : expectedLeaseInfos) {
	// ClientLeaseInfo actualClientLeaseInfo = actualLeaseInfos.get(expectedLeaseInfo.getKey());
	// ClientLeaseInfo expectedClientLeaseInfo = expectedLeaseInfo.getValue();
	// assertEquals(expectedClientLeaseInfo.getIp(), actualClientLeaseInfo.getIp());
	// assertEquals(expectedClientLeaseInfo.getPort(), actualClientLeaseInfo.getPort());
	//
	// Lease actualLease = actualClientLeaseInfo.getLease();
	// Lease expectedLease = expectedClientLeaseInfo.getLease();
	// assertEquals(expectedLease.getId(), actualLease.getId());
	// assertEquals(expectedLease.getExpireTime(), actualLease.getExpireTime());
	// }
	// }
	//
	// private void configureBrokerLeaseHolder() throws Exception {
	// defineComponent(BrokerLeaseHolder.class, TestBrokerLeaaseHolder.class)//
	// .req(ZKClient.class)//
	// .req(ZookeeperService.class)//
	// .req(SystemClockService.class);
	//
	// m_leaseHolder = lookup(BrokerLeaseHolder.class);
	// }
	//
	// @Override
	// protected void initZkData() throws Exception {
	// ensurePath(ZKPathUtils.getBrokerLeaseRootZkPath());
	// }
	//
	// // private void leaseHolderReload() throws Exception {
	// // m_leaseHolder.updateContexts(m_leaseHolder.loadExistingLeases());
	// // }
	//
	// @Override
	// protected void doSetUp() throws Exception {
	// configureBrokerLeaseHolder();
	// }
	//
	// // @Test
	// // public void testExecuteLeaseOperation() throws Exception {
	// // final long fakeNowTimestamp = System.currentTimeMillis() + 500 * 1000L;
	// //
	// // addLeasesToZk("t1", 0, Arrays.asList(//
	// // new Pair<String, ClientLeaseInfo>("br0", new ClientLeaseInfo(new Lease(1, fakeNowTimestamp + 50),
	// // "0.0.0.0", 1234)),//
	// // new Pair<String, ClientLeaseInfo>("br1", new ClientLeaseInfo(new Lease(2, 30), "0.0.0.1", 1233))//
	// // ));
	// //
	// // leaseHolderReload();
	// //
	// // m_leaseHolder.executeLeaseOperation(new Pair<String, Integer>("t1", 0), new LeaseOperationCallback() {
	// //
	// // @Override
	// // public LeaseAcquireResponse execute(Map<String, ClientLeaseInfo> existingValidLeases) throws Exception {
	// // assertEquals(1, existingValidLeases.size());
	// // ClientLeaseInfo br0ClientLeaseInfo = existingValidLeases.get("br0");
	// // ClientLeaseInfo br1ClientLeaseInfo = existingValidLeases.get("br1");
	// // assertNotNull(br0ClientLeaseInfo);
	// // assertNull(br1ClientLeaseInfo);
	// //
	// // Map<Pair<String, Integer>, Map<String, ClientLeaseInfo>> leases = new HashMap<>();
	// // leases.put(new Pair<String, Integer>("t1", 0), existingValidLeases);
	// //
	// // assertLeases(
	// // leases,
	// // "t1",
	// // 0,
	// // Arrays.asList(//
	// // new Pair<String, ClientLeaseInfo>("br0", new ClientLeaseInfo(new Lease(1, fakeNowTimestamp + 50),
	// // "0.0.0.0", 1234))));
	// //
	// // return null;
	// // }
	// // });
	// // }
	//
	// // @Test
	// // public void testInit() throws Exception {
	// // long fakeNowTimestamp = System.currentTimeMillis() + 500 * 1000L;
	// //
	// // addLeasesToZk("t1", 0, Arrays.asList(//
	// // new Pair<String, ClientLeaseInfo>("br0", new ClientLeaseInfo(new Lease(1, fakeNowTimestamp + 50),
	// // "0.0.0.0", 1234)),//
	// // new Pair<String, ClientLeaseInfo>("br1", new ClientLeaseInfo(new Lease(2, fakeNowTimestamp + 30),
	// // "0.0.0.1", 1233))//
	// // ));
	// //
	// // addLeasesToZk(
	// // "t1",
	// // 1,
	// // Arrays.asList(//
	// // new Pair<String, ClientLeaseInfo>("br2", new ClientLeaseInfo(new Lease(3, fakeNowTimestamp + 150),
	// // "0.0.0.2", 2222))//
	// // ));
	// //
	// // addLeasesToZk("t1", 2, null);
	// //
	// // addLeasesToZk(
	// // "t2",
	// // 0,
	// // Arrays.asList(//
	// // new Pair<String, ClientLeaseInfo>("br0", new ClientLeaseInfo(new Lease(1, fakeNowTimestamp + 50),
	// // "0.0.0.0", 1234))//
	// // ));
	// //
	// // addLeasesToZk("t3", 2, null);
	// //
	// // leaseHolderReload();
	// //
	// // Map<Pair<String, Integer>, Map<String, ClientLeaseInfo>> allValidLeases = m_leaseHolder.getAllValidLeases();
	// // assertEquals(3, allValidLeases.size());
	// //
	// // assertLeases(allValidLeases, "t1", 0, Arrays.asList(//
	// // new Pair<String, ClientLeaseInfo>("br0", new ClientLeaseInfo(new Lease(1, fakeNowTimestamp + 50),
	// // "0.0.0.0", 1234)),//
	// // new Pair<String, ClientLeaseInfo>("br1", new ClientLeaseInfo(new Lease(2, fakeNowTimestamp + 30),
	// // "0.0.0.1", 1233))//
	// // ));
	// // assertLeases(
	// // allValidLeases,
	// // "t1",
	// // 1,
	// // Arrays.asList(//
	// // new Pair<String, ClientLeaseInfo>("br2", new ClientLeaseInfo(new Lease(3, fakeNowTimestamp + 150),
	// // "0.0.0.2", 2222))//
	// // ));
	// // assertLeases(
	// // allValidLeases,
	// // "t2",
	// // 0,
	// // Arrays.asList(//
	// // new Pair<String, ClientLeaseInfo>("br0", new ClientLeaseInfo(new Lease(1, fakeNowTimestamp + 50),
	// // "0.0.0.0", 1234))//
	// // ));
	// //
	// // assertTrue(m_leaseHolder.topicWatched("t1"));
	// // assertTrue(m_leaseHolder.topicWatched("t2"));
	// // assertTrue(m_leaseHolder.topicWatched("t3"));
	// // assertFalse(m_leaseHolder.topicWatched("t4"));
	// // }
	//
	// @Test
	// public void testInitWithoutData() throws Exception {
	// assertTrue(m_leaseHolder.getAllValidLeases().isEmpty());
	// }
	//
	// // @Test
	// // public void testLeaseAddedInZk() throws Exception {
	// // leaseHolderReload();
	// //
	// // long fakeNowTimestamp = System.currentTimeMillis();
	// //
	// // String topic = "t1";
	// // int partition = 0;
	// // String brokerName = "br0";
	// // String ip = "0.0.0.1";
	// // int port = 1234;
	// // long expireTime = fakeNowTimestamp + 50000L;
	// // addLeasesToZk(topic, partition, Arrays.asList(//
	// // new Pair<String, ClientLeaseInfo>(brokerName, new ClientLeaseInfo(new Lease(1, expireTime), ip, port))//
	// // ));
	// //
	// // Map<Pair<String, Integer>, Map<String, ClientLeaseInfo>> allValidLeases = m_leaseHolder.getAllValidLeases();
	// //
	// // int retries = 50;
	// // int i = 0;
	// // while (i++ < retries && allValidLeases.isEmpty()) {
	// // TimeUnit.MILLISECONDS.sleep(100);
	// // allValidLeases = m_leaseHolder.getAllValidLeases();
	// // }
	// //
	// // assertEquals(1, allValidLeases.size());
	// // assertTrue(m_leaseHolder.topicWatched(topic));
	// //
	// // assertLeases(allValidLeases, topic, partition, Arrays.asList(//
	// // new Pair<String, ClientLeaseInfo>(brokerName, new ClientLeaseInfo(new Lease(1, expireTime), ip, port))));
	// // }
	//
	// @Test
	// public void testLeaseChangedInZk() throws Exception {
	//
	// long fakeNowTimestamp = System.currentTimeMillis();
	// String topic = "t1";
	// int partition = 0;
	// String brokerName = "br0";
	// String ip = "0.0.0.1";
	// int port = 1234;
	// long expireTime = fakeNowTimestamp + 50000L;
	// int retries = 50;
	//
	// addLeasesToZk(
	// topic,
	// partition,
	// Arrays.asList(//
	// new Pair<String, ClientLeaseInfo>(brokerName + "a", new ClientLeaseInfo(new Lease(2, System
	// .currentTimeMillis() + 500L), ip + "2", port + 1))//
	// ));
	//
	// Map<Pair<String, Integer>, Map<String, ClientLeaseInfo>> allValidLeases = m_leaseHolder.getAllValidLeases();
	//
	// for (int i = 0; i < retries && allValidLeases.isEmpty(); i++) {
	// TimeUnit.MILLISECONDS.sleep(100);
	// allValidLeases = m_leaseHolder.getAllValidLeases();
	// }
	//
	// allValidLeases.clear();
	//
	// addLeasesToZk(topic, partition, Arrays.asList(//
	// new Pair<String, ClientLeaseInfo>(brokerName, new ClientLeaseInfo(new Lease(1, expireTime), ip, port))//
	// ));
	//
	// for (int i = 0; i < retries; i++) {
	// TimeUnit.MILLISECONDS.sleep(100);
	// allValidLeases = m_leaseHolder.getAllValidLeases();
	// if (!allValidLeases.isEmpty()
	// && allValidLeases.get(new Pair<String, Integer>(topic, partition)).containsKey(brokerName)) {
	// break;
	// }
	// }
	//
	// assertEquals(1, allValidLeases.size());
	//
	// assertLeases(allValidLeases, topic, partition, Arrays.asList(//
	// new Pair<String, ClientLeaseInfo>(brokerName, new ClientLeaseInfo(new Lease(1, expireTime), ip, port))));
	// }
	//
	// // @Test
	// // public void testLeaseRemovedInZk() throws Exception {
	// //
	// // long fakeNowTimestamp = System.currentTimeMillis();
	// // String topic = "t1";
	// // int partition = 0;
	// // String brokerName = "br0";
	// // String ip = "0.0.0.1";
	// // int port = 1234;
	// // long expireTime = fakeNowTimestamp + 1000L;
	// // int retries = 50;
	// //
	// // addLeasesToZk(topic, partition, Arrays.asList(//
	// // new Pair<String, ClientLeaseInfo>(brokerName, new ClientLeaseInfo(new Lease(1, expireTime), ip, port))//
	// // ));
	// //
	// // Map<Pair<String, Integer>, Map<String, ClientLeaseInfo>> allValidLeases = m_leaseHolder.getAllValidLeases();
	// //
	// // for (int i = 0; i < retries && allValidLeases.isEmpty(); i++) {
	// // TimeUnit.MILLISECONDS.sleep(100);
	// // allValidLeases = m_leaseHolder.getAllValidLeases();
	// // }
	// //
	// // assertEquals(1, allValidLeases.size());
	// // allValidLeases.clear();
	// //
	// // deleteChildren(ZKPathUtils.getBrokerLeaseTopicParentZkPath(topic), true);
	// //
	// // for (int i = 0; i < retries; i++) {
	// // TimeUnit.MILLISECONDS.sleep(100);
	// // allValidLeases = m_leaseHolder.getAllValidLeases();
	// // if (allValidLeases.isEmpty()) {
	// // break;
	// // }
	// // }
	// //
	// // assertFalse(m_leaseHolder.topicWatched(topic));
	// // assertTrue(allValidLeases.isEmpty());
	// //
	// // }
	//
	// // @Test
	// // public void testNewLease() throws Exception {
	// // final String topic = "t1";
	// // final int partition = 1;
	// // final String ip = "1.1.1.2";
	// // final int port = 1111;
	// // String brokerName = "br0";
	// //
	// // leaseHolderReload();
	// //
	// // Map<String, ClientLeaseInfo> existingValidLeases = new HashMap<>();
	// // m_leaseHolder.newLease(new Pair<String, Integer>(topic, partition), brokerName, existingValidLeases,
	// // 1000 * 1000L, ip, port);
	// //
	// // assertEquals(1, existingValidLeases.size());
	// // ClientLeaseInfo clientLeaseInfo = existingValidLeases.get(brokerName);
	// // assertNotNull(clientLeaseInfo);
	// //
	// // assertEquals(ip, clientLeaseInfo.getIp());
	// // assertEquals(port, clientLeaseInfo.getPort());
	// // assertFalse(clientLeaseInfo.getLease().isExpired());
	// //
	// // Map<String, ClientLeaseInfo> zkLeases = ZKSerializeUtils.deserialize(
	// // m_curator.getData().forPath(ZKPathUtils.getBrokerLeaseZkPath(topic, partition)),
	// // new TypeReference<Map<String, ClientLeaseInfo>>() {
	// // }.getType());
	// //
	// // assertEquals(1, zkLeases.size());
	// // clientLeaseInfo = zkLeases.get(brokerName);
	// // assertNotNull(clientLeaseInfo);
	// //
	// // assertEquals(ip, clientLeaseInfo.getIp());
	// // assertEquals(port, clientLeaseInfo.getPort());
	// // assertFalse(clientLeaseInfo.getLease().isExpired());
	// // }
	// //
	// // @Test
	// // public void testRemoveExpiredLeasesAndGetAllValidLeases() throws Exception {
	// // long fakeNowTimestamp = System.currentTimeMillis() + 500 * 1000L;
	// //
	// // addLeasesToZk("t1", 0, Arrays.asList(//
	// // new Pair<String, ClientLeaseInfo>("br0", new ClientLeaseInfo(new Lease(1, fakeNowTimestamp + 50),
	// // "0.0.0.0", 1234)),//
	// // new Pair<String, ClientLeaseInfo>("br1", new ClientLeaseInfo(new Lease(2, fakeNowTimestamp + 30),
	// // "0.0.0.1", 1233))//
	// // ));
	// //
	// // addLeasesToZk("t1", 1, Arrays.asList(//
	// // new Pair<String, ClientLeaseInfo>("br2", new ClientLeaseInfo(new Lease(3, 0), "0.0.0.2", 2222))//
	// // ));
	// //
	// // addLeasesToZk("t2", 0, Arrays.asList(//
	// // new Pair<String, ClientLeaseInfo>("br0", new ClientLeaseInfo(new Lease(1, 0), "0.0.0.0", 1234))//
	// // ));
	// //
	// // leaseHolderReload();
	// //
	// // Map<Pair<String, Integer>, Map<String, ClientLeaseInfo>> allValidLeases = m_leaseHolder.getAllValidLeases();
	// // assertEquals(1, allValidLeases.size());
	// //
	// // assertLeases(allValidLeases, "t1", 0, Arrays.asList(//
	// // new Pair<String, ClientLeaseInfo>("br0", new ClientLeaseInfo(new Lease(1, fakeNowTimestamp + 50),
	// // "0.0.0.0", 1234)),//
	// // new Pair<String, ClientLeaseInfo>("br1", new ClientLeaseInfo(new Lease(2, fakeNowTimestamp + 30),
	// // "0.0.0.1", 1233))//
	// // ));
	// // }
	// //
	// // @Test
	// // public void testRenewLease() throws Exception {
	// // final String topic = "t1";
	// // final int partition = 1;
	// // final String ip = "1.1.1.2";
	// // final int port = 1111;
	// // String brokerName = "br0";
	// //
	// // leaseHolderReload();
	// //
	// // long now = System.currentTimeMillis();
	// //
	// // Map<String, ClientLeaseInfo> existingValidLeases = new HashMap<>();
	// // ClientLeaseInfo existingLeaseInfo = new ClientLeaseInfo(new Lease(1, now + 1000L), ip, port);
	// //
	// // m_leaseHolder.renewLease(new Pair<String, Integer>(topic, partition), brokerName, existingValidLeases,
	// // existingLeaseInfo, 1000 * 1000L, ip, port);
	// //
	// // assertEquals(1, existingValidLeases.size());
	// // ClientLeaseInfo clientLeaseInfo = existingValidLeases.get(brokerName);
	// // assertNotNull(clientLeaseInfo);
	// //
	// // assertEquals(ip, clientLeaseInfo.getIp());
	// // assertEquals(port, clientLeaseInfo.getPort());
	// // assertFalse(clientLeaseInfo.getLease().isExpired());
	// // assertEquals(1, clientLeaseInfo.getLease().getId());
	// // assertEquals(now + 1000L + 1000 * 1000L, clientLeaseInfo.getLease().getExpireTime());
	// //
	// // Map<String, ClientLeaseInfo> zkLeases = ZKSerializeUtils.deserialize(
	// // m_curator.getData().forPath(ZKPathUtils.getBrokerLeaseZkPath(topic, partition)),
	// // new TypeReference<Map<String, ClientLeaseInfo>>() {
	// // }.getType());
	// //
	// // assertEquals(1, zkLeases.size());
	// // clientLeaseInfo = zkLeases.get(brokerName);
	// // assertNotNull(clientLeaseInfo);
	// //
	// // assertEquals(ip, clientLeaseInfo.getIp());
	// // assertEquals(port, clientLeaseInfo.getPort());
	// // assertFalse(clientLeaseInfo.getLease().isExpired());
	// // assertEquals(1, clientLeaseInfo.getLease().getId());
	// // assertEquals(now + 1000L + 1000 * 1000L, clientLeaseInfo.getLease().getExpireTime());
	// // }

}
