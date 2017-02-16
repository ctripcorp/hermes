package com.ctrip.hermes.metaserver.consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.metaserver.ZKSuppportTestCase;
import com.ctrip.hermes.metaserver.commons.ClientLeaseInfo;
import com.ctrip.hermes.metaservice.service.ZookeeperService;
import com.ctrip.hermes.metaservice.zk.ZKClient;
import com.ctrip.hermes.metaservice.zk.ZKPathUtils;
import com.ctrip.hermes.metaservice.zk.ZKSerializeUtils;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class ConsumerLeaseHolderTest{// extends ZKSuppportTestCase {

	// private ConsumerLeaseHolder m_leaseHolder;
	//
	// public static class TestConsumerLeaaseHolder extends ConsumerLeaseHolder {
	// }
	//
	// private void addLeasesToZk(Tpg tpg, List<Pair<String, ClientLeaseInfo>> data) throws Exception {
	// String path = ZKPathUtils.getConsumerLeaseZkPath(tpg.getTopic(), tpg.getPartition(), tpg.getGroupId());
	// String parentPath = ZKPathUtils.getConsumerLeaseTopicParentZkPath(tpg.getTopic());
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
	// ensurePath(parentPath + "/" + tpg.getPartition());
	// ensurePath(path);
	//
	// m_curator.setData().forPath(path, bytes);
	// m_curator.setData().forPath(parentPath, ZKSerializeUtils.serialize(System.currentTimeMillis()));
	// }
	//
	// private void assertLeases(Map<Tpg, Map<String, ClientLeaseInfo>> allValidLeases, Tpg tpg,
	// List<Pair<String, ClientLeaseInfo>> expectedLeaseInfos) {
	// Map<String, ClientLeaseInfo> actualLeaseInfos = allValidLeases.get(tpg);
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
	// private void configureConsumerLeaseHolder() throws Exception {
	// defineComponent(ConsumerLeaseHolder.class, TestConsumerLeaaseHolder.class)//
	// .req(ZKClient.class)//
	// .req(ZookeeperService.class)//
	// .req(SystemClockService.class);
	//
	// m_leaseHolder = lookup(ConsumerLeaseHolder.class);
	// }
	//
	// @Override
	// protected void initZkData() throws Exception {
	// ensurePath(ZKPathUtils.getConsumerLeaseRootZkPath());
	// }
	//
	// // private void leaseHolderReload() throws Exception {
	// // m_leaseHolder.updateContexts(m_leaseHolder.loadExistingLeases());
	// // }
	//
	// @Override
	// protected void doSetUp() throws Exception {
	// configureConsumerLeaseHolder();
	// }
	//
	// // @Test
	// // public void testExecuteLeaseOperation() throws Exception {
	// // final long fakeNowTimestamp = System.currentTimeMillis() + 500 * 1000L;
	// //
	// // final Tpg tpg = new Tpg("t1", 0, "g1");
	// //
	// // addLeasesToZk(tpg, Arrays.asList(//
	// // new Pair<String, ClientLeaseInfo>("c0", new ClientLeaseInfo(new Lease(1, fakeNowTimestamp + 500),
	// // "0.0.0.0", 1234)),//
	// // new Pair<String, ClientLeaseInfo>("c1", new ClientLeaseInfo(new Lease(2, 30), "0.0.0.1", 1233))//
	// // ));
	// //
	// // leaseHolderReload();
	// //
	// // m_leaseHolder.executeLeaseOperation(tpg, new LeaseOperationCallback() {
	// //
	// // @Override
	// // public LeaseAcquireResponse execute(Map<String, ClientLeaseInfo> existingValidLeases) throws Exception {
	// // assertEquals(1, existingValidLeases.size());
	// // ClientLeaseInfo c0ClientLeaseInfo = existingValidLeases.get("c0");
	// // ClientLeaseInfo c1ClientLeaseInfo = existingValidLeases.get("c1");
	// // assertNotNull(c0ClientLeaseInfo);
	// // assertNull(c1ClientLeaseInfo);
	// //
	// // Map<Tpg, Map<String, ClientLeaseInfo>> leases = new HashMap<>();
	// // leases.put(tpg, existingValidLeases);
	// //
	// // assertLeases(
	// // leases,
	// // tpg,
	// // Arrays.asList(//
	// // new Pair<String, ClientLeaseInfo>("c0", new ClientLeaseInfo(new Lease(1, fakeNowTimestamp + 500),
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
	// // Tpg t1p0g1 = new Tpg("t1", 0, "g1");
	// // Tpg t1p0g2 = new Tpg("t1", 0, "g2");
	// // Tpg t1p1g1 = new Tpg("t1", 1, "g1");
	// // Tpg t1p2g1 = new Tpg("t1", 2, "g1");
	// // Tpg t2p0g1 = new Tpg("t2", 0, "g1");
	// // Tpg t3p0g1 = new Tpg("t3", 0, "g1");
	// //
	// // addLeasesToZk(t1p0g1, Arrays.asList(//
	// // new Pair<String, ClientLeaseInfo>("c0", new ClientLeaseInfo(new Lease(1, fakeNowTimestamp + 50), "0.0.0.0",
	// // 1234)),//
	// // new Pair<String, ClientLeaseInfo>("c1", new ClientLeaseInfo(new Lease(2, fakeNowTimestamp + 30), "0.0.0.1",
	// // 1233))//
	// // ));
	// //
	// // addLeasesToZk(t1p0g2, Arrays.asList(//
	// // new Pair<String, ClientLeaseInfo>("c2", new ClientLeaseInfo(new Lease(3, fakeNowTimestamp + 150),
	// // "0.0.0.2", 2222)),//
	// // new Pair<String, ClientLeaseInfo>("c1", new ClientLeaseInfo(new Lease(2, fakeNowTimestamp + 30), "0.0.0.1",
	// // 1233))//
	// // ));
	// //
	// // addLeasesToZk(
	// // t1p1g1,
	// // Arrays.asList(//
	// // new Pair<String, ClientLeaseInfo>("c2", new ClientLeaseInfo(new Lease(3, fakeNowTimestamp + 150),
	// // "0.0.0.2", 2222))//
	// // ));
	// //
	// // addLeasesToZk(t1p2g1, null);
	// //
	// // addLeasesToZk(
	// // t2p0g1,
	// // Arrays.asList(//
	// // new Pair<String, ClientLeaseInfo>("c0", new ClientLeaseInfo(new Lease(1, fakeNowTimestamp + 50), "0.0.0.0",
	// // 1234))//
	// // ));
	// //
	// // addLeasesToZk(t3p0g1, null);
	// //
	// // leaseHolderReload();
	// //
	// // Map<Tpg, Map<String, ClientLeaseInfo>> allValidLeases = m_leaseHolder.getAllValidLeases();
	// // assertEquals(4, allValidLeases.size());
	// //
	// // assertLeases(allValidLeases, t1p0g1, Arrays.asList(//
	// // new Pair<String, ClientLeaseInfo>("c0", new ClientLeaseInfo(new Lease(1, fakeNowTimestamp + 50), "0.0.0.0",
	// // 1234)),//
	// // new Pair<String, ClientLeaseInfo>("c1", new ClientLeaseInfo(new Lease(2, fakeNowTimestamp + 30), "0.0.0.1",
	// // 1233))//
	// // ));
	// // assertLeases(allValidLeases, t1p0g2, Arrays.asList(//
	// // new Pair<String, ClientLeaseInfo>("c2", new ClientLeaseInfo(new Lease(3, fakeNowTimestamp + 150),
	// // "0.0.0.2", 2222)),//
	// // new Pair<String, ClientLeaseInfo>("c1", new ClientLeaseInfo(new Lease(2, fakeNowTimestamp + 30), "0.0.0.1",
	// // 1233))//
	// // ));
	// // assertLeases(
	// // allValidLeases,
	// // t1p1g1,
	// // Arrays.asList(//
	// // new Pair<String, ClientLeaseInfo>("c2", new ClientLeaseInfo(new Lease(3, fakeNowTimestamp + 150),
	// // "0.0.0.2", 2222))//
	// // ));
	// // assertLeases(
	// // allValidLeases,
	// // t2p0g1,
	// // Arrays.asList(//
	// // new Pair<String, ClientLeaseInfo>("c0", new ClientLeaseInfo(new Lease(1, fakeNowTimestamp + 50), "0.0.0.0",
	// // 1234))//
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
	// // Tpg tpg = new Tpg("t1", 0, "g1");
	// // String consumerName = "c0";
	// // String ip = "0.0.0.1";
	// // int port = 1234;
	// // long expireTime = fakeNowTimestamp + 50000L;
	// // addLeasesToZk(tpg, Arrays.asList(//
	// // new Pair<String, ClientLeaseInfo>(consumerName, new ClientLeaseInfo(new Lease(1, expireTime), ip, port))//
	// // ));
	// //
	// // Map<Tpg, Map<String, ClientLeaseInfo>> allValidLeases = m_leaseHolder.getAllValidLeases();
	// //
	// // int retries = 50;
	// // int i = 0;
	// // while (i++ < retries && allValidLeases.isEmpty()) {
	// // TimeUnit.MILLISECONDS.sleep(100);
	// // allValidLeases = m_leaseHolder.getAllValidLeases();
	// // }
	// //
	// // assertEquals(1, allValidLeases.size());
	// // assertTrue(m_leaseHolder.topicWatched(tpg.getTopic()));
	// //
	// // assertLeases(allValidLeases, tpg, Arrays.asList(//
	// // new Pair<String, ClientLeaseInfo>(consumerName, new ClientLeaseInfo(new Lease(1, expireTime), ip, port))));
	// // }
	//
	// @Test
	// public void testLeaseChangedInZk() throws Exception {
	//
	// long fakeNowTimestamp = System.currentTimeMillis();
	// Tpg tpg = new Tpg("t1", 0, "g1");
	// String consumerName = "c0";
	// String ip = "0.0.0.1";
	// int port = 1234;
	// long expireTime = fakeNowTimestamp + 50000L;
	// int retries = 50;
	//
	// addLeasesToZk(
	// tpg,
	// Arrays.asList(//
	// new Pair<String, ClientLeaseInfo>(consumerName + "a", new ClientLeaseInfo(new Lease(2, System
	// .currentTimeMillis() + 500L), ip + "2", port + 1))//
	// ));
	//
	// Map<Tpg, Map<String, ClientLeaseInfo>> allValidLeases = m_leaseHolder.getAllValidLeases();
	//
	// for (int i = 0; i < retries && allValidLeases.isEmpty(); i++) {
	// TimeUnit.MILLISECONDS.sleep(100);
	// allValidLeases = m_leaseHolder.getAllValidLeases();
	// }
	//
	// allValidLeases.clear();
	//
	// addLeasesToZk(tpg, Arrays.asList(//
	// new Pair<String, ClientLeaseInfo>(consumerName, new ClientLeaseInfo(new Lease(1, expireTime), ip, port))//
	// ));
	//
	// for (int i = 0; i < retries; i++) {
	// TimeUnit.MILLISECONDS.sleep(100);
	// allValidLeases = m_leaseHolder.getAllValidLeases();
	// if (!allValidLeases.isEmpty() && allValidLeases.get(tpg).containsKey(consumerName)) {
	// break;
	// }
	// }
	//
	// assertEquals(1, allValidLeases.size());
	//
	// assertLeases(allValidLeases, tpg, Arrays.asList(//
	// new Pair<String, ClientLeaseInfo>(consumerName, new ClientLeaseInfo(new Lease(1, expireTime), ip, port))));
	// }
	//
	// // @Test
	// // public void testLeaseRemovedInZk() throws Exception {
	// //
	// // long fakeNowTimestamp = System.currentTimeMillis();
	// // Tpg tpg = new Tpg("t1", 0, "g1");
	// // String consumerName = "c0";
	// // String ip = "0.0.0.1";
	// // int port = 1234;
	// // long expireTime = fakeNowTimestamp + 1000L;
	// // int retries = 50;
	// //
	// // addLeasesToZk(tpg, Arrays.asList(//
	// // new Pair<String, ClientLeaseInfo>(consumerName, new ClientLeaseInfo(new Lease(1, expireTime), ip, port))//
	// // ));
	// //
	// // Map<Tpg, Map<String, ClientLeaseInfo>> allValidLeases = m_leaseHolder.getAllValidLeases();
	// //
	// // for (int i = 0; i < retries && allValidLeases.isEmpty(); i++) {
	// // TimeUnit.MILLISECONDS.sleep(100);
	// // allValidLeases = m_leaseHolder.getAllValidLeases();
	// // }
	// //
	// // assertEquals(1, allValidLeases.size());
	// // allValidLeases.clear();
	// //
	// // deleteChildren(ZKPathUtils.getConsumerLeaseTopicParentZkPath(tpg.getTopic()), true);
	// //
	// // for (int i = 0; i < retries; i++) {
	// // TimeUnit.MILLISECONDS.sleep(100);
	// // allValidLeases = m_leaseHolder.getAllValidLeases();
	// // if (allValidLeases.isEmpty()) {
	// // break;
	// // }
	// // }
	// //
	// // assertFalse(m_leaseHolder.topicWatched(tpg.getTopic()));
	// // assertTrue(allValidLeases.isEmpty());
	// //
	// // }
	//
	// // @Test
	// // public void testNewLease() throws Exception {
	// // Tpg tpg = new Tpg("t1", 0, "g1");
	// // String consumerName = "c0";
	// // final String ip = "1.1.1.2";
	// // final int port = 1111;
	// //
	// // leaseHolderReload();
	// //
	// // Map<String, ClientLeaseInfo> existingValidLeases = new HashMap<>();
	// // m_leaseHolder.newLease(tpg, consumerName, existingValidLeases, 1000 * 1000L, ip, port);
	// //
	// // assertEquals(1, existingValidLeases.size());
	// // ClientLeaseInfo clientLeaseInfo = existingValidLeases.get(consumerName);
	// // assertNotNull(clientLeaseInfo);
	// //
	// // assertEquals(ip, clientLeaseInfo.getIp());
	// // assertEquals(port, clientLeaseInfo.getPort());
	// // assertFalse(clientLeaseInfo.getLease().isExpired());
	// //
	// // Map<String, ClientLeaseInfo> zkLeases = ZKSerializeUtils.deserialize(
	// // m_curator.getData().forPath(
	// // ZKPathUtils.getConsumerLeaseZkPath(tpg.getTopic(), tpg.getPartition(), tpg.getGroupId())),
	// // new TypeReference<Map<String, ClientLeaseInfo>>() {
	// // }.getType());
	// //
	// // assertEquals(1, zkLeases.size());
	// // clientLeaseInfo = zkLeases.get(consumerName);
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
	// // Tpg t1p0g1 = new Tpg("t1", 0, "g1");
	// // Tpg t1p1g1 = new Tpg("t1", 1, "g1");
	// // Tpg t1p0g2 = new Tpg("t1", 0, "g2");
	// //
	// // addLeasesToZk(t1p0g1, Arrays.asList(//
	// // new Pair<String, ClientLeaseInfo>("c0", new ClientLeaseInfo(new Lease(1, fakeNowTimestamp + 50), "0.0.0.0",
	// // 1234)),//
	// // new Pair<String, ClientLeaseInfo>("c1", new ClientLeaseInfo(new Lease(2, fakeNowTimestamp + 30), "0.0.0.1",
	// // 1233))//
	// // ));
	// //
	// // addLeasesToZk(t1p1g1, Arrays.asList(//
	// // new Pair<String, ClientLeaseInfo>("c2", new ClientLeaseInfo(new Lease(3, 0), "0.0.0.2", 2222))//
	// // ));
	// //
	// // addLeasesToZk(t1p0g2, Arrays.asList(//
	// // new Pair<String, ClientLeaseInfo>("c0", new ClientLeaseInfo(new Lease(1, 0), "0.0.0.0", 1234))//
	// // ));
	// //
	// // leaseHolderReload();
	// //
	// // int retries = 50;
	// // int i = 0;
	// // Map<Tpg, Map<String, ClientLeaseInfo>> allValidLeases = null;
	// // while (i++ < retries) {
	// // allValidLeases = m_leaseHolder.getAllValidLeases();
	// // if (allValidLeases.size() == 1) {
	// // break;
	// // } else {
	// // TimeUnit.MILLISECONDS.sleep(100);
	// // }
	// // }
	// // assertEquals(1, allValidLeases.size());
	// //
	// // assertLeases(allValidLeases, t1p0g1, Arrays.asList(//
	// // new Pair<String, ClientLeaseInfo>("c0", new ClientLeaseInfo(new Lease(1, fakeNowTimestamp + 50), "0.0.0.0",
	// // 1234)),//
	// // new Pair<String, ClientLeaseInfo>("c1", new ClientLeaseInfo(new Lease(2, fakeNowTimestamp + 30), "0.0.0.1",
	// // 1233))//
	// // ));
	// // }
	// //
	// // @Test
	// // public void testRenewLease() throws Exception {
	// //
	// // Tpg tpg = new Tpg("t1", 1, "g1");
	// // final String ip = "1.1.1.2";
	// // final int port = 1111;
	// // String consumerName = "c0";
	// //
	// // leaseHolderReload();
	// //
	// // long now = System.currentTimeMillis();
	// //
	// // Map<String, ClientLeaseInfo> existingValidLeases = new HashMap<>();
	// // ClientLeaseInfo existingLeaseInfo = new ClientLeaseInfo(new Lease(1, now + 1000L), ip, port);
	// //
	// // m_leaseHolder.renewLease(tpg, consumerName, existingValidLeases, existingLeaseInfo, 1000 * 1000L, ip, port);
	// //
	// // assertEquals(1, existingValidLeases.size());
	// // ClientLeaseInfo clientLeaseInfo = existingValidLeases.get(consumerName);
	// // assertNotNull(clientLeaseInfo);
	// //
	// // assertEquals(ip, clientLeaseInfo.getIp());
	// // assertEquals(port, clientLeaseInfo.getPort());
	// // assertFalse(clientLeaseInfo.getLease().isExpired());
	// // assertEquals(1, clientLeaseInfo.getLease().getId());
	// // assertEquals(now + 1000L + 1000 * 1000L, clientLeaseInfo.getLease().getExpireTime());
	// //
	// // Map<String, ClientLeaseInfo> zkLeases = ZKSerializeUtils.deserialize(
	// // m_curator.getData().forPath(
	// // ZKPathUtils.getConsumerLeaseZkPath(tpg.getTopic(), tpg.getPartition(), tpg.getGroupId())),
	// // new TypeReference<Map<String, ClientLeaseInfo>>() {
	// // }.getType());
	// //
	// // assertEquals(1, zkLeases.size());
	// // clientLeaseInfo = zkLeases.get(consumerName);
	// // assertNotNull(clientLeaseInfo);
	// //
	// // assertEquals(ip, clientLeaseInfo.getIp());
	// // assertEquals(port, clientLeaseInfo.getPort());
	// // assertFalse(clientLeaseInfo.getLease().isExpired());
	// // assertEquals(1, clientLeaseInfo.getLease().getId());
	// // assertEquals(now + 1000L + 1000 * 1000L, clientLeaseInfo.getLease().getExpireTime());
	// // }

}
