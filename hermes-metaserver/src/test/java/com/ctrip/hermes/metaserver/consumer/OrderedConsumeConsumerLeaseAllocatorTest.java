package com.ctrip.hermes.metaserver.consumer;

import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class OrderedConsumeConsumerLeaseAllocatorTest {
	// private static final long DEFAULT_LEASE_RETRY_DELAY = 1000L;
	//
	// private static final long DEFAULT_LEASE_TIME = 5000L;
	//
	// private static final long DEFAULT_LEASE_ADJUST_TIME = -123L;
	//
	// @Mock
	// private MetaServerConfig m_config;
	//
	// @Mock
	// private SystemClockService m_systemClockService;
	//
	// @Mock
	// private ConsumerLeaseHolder m_leaseHolder;
	//
	// @Mock
	// private ConsumerAssignmentHolder m_assignmentHolder;
	//
	// @Mock
	// private ActiveConsumerListHolder m_activeConsumerList;
	//
	// private DefaultConsumerLeaseAllocator m_allocator;
	//
	// private List<HeartbeatInfo> m_heartbeats = new LinkedList<>();
	//
	// private static class HeartbeatInfo {
	// private String m_topic;
	//
	// private String m_group;
	//
	// private String m_consumerName;
	//
	// private String m_ip;
	//
	// public HeartbeatInfo(String topic, String group, String consumerName, String ip) {
	// m_topic = topic;
	// m_group = group;
	// m_consumerName = consumerName;
	// m_ip = ip;
	// }
	//
	// @Override
	// public int hashCode() {
	// final int prime = 31;
	// int result = 1;
	// result = prime * result + ((m_consumerName == null) ? 0 : m_consumerName.hashCode());
	// result = prime * result + ((m_group == null) ? 0 : m_group.hashCode());
	// result = prime * result + ((m_ip == null) ? 0 : m_ip.hashCode());
	// result = prime * result + ((m_topic == null) ? 0 : m_topic.hashCode());
	// return result;
	// }
	//
	// @Override
	// public boolean equals(Object obj) {
	// if (this == obj)
	// return true;
	// if (obj == null)
	// return false;
	// if (getClass() != obj.getClass())
	// return false;
	// HeartbeatInfo other = (HeartbeatInfo) obj;
	// if (m_consumerName == null) {
	// if (other.m_consumerName != null)
	// return false;
	// } else if (!m_consumerName.equals(other.m_consumerName))
	// return false;
	// if (m_group == null) {
	// if (other.m_group != null)
	// return false;
	// } else if (!m_group.equals(other.m_group))
	// return false;
	// if (m_ip == null) {
	// if (other.m_ip != null)
	// return false;
	// } else if (!m_ip.equals(other.m_ip))
	// return false;
	// if (m_topic == null) {
	// if (other.m_topic != null)
	// return false;
	// } else if (!m_topic.equals(other.m_topic))
	// return false;
	// return true;
	// }
	//
	// }
	//
	// @After
	// public void after() throws Exception {
	// m_heartbeats.clear();
	// }
	//
	// @Before
	// public void before() throws Exception {
	// m_allocator = new DefaultConsumerLeaseAllocator();
	// m_allocator.setAssignmentHolder(m_assignmentHolder);
	// m_allocator.setConfig(m_config);
	// m_allocator.setLeaseHolder(m_leaseHolder);
	// m_allocator.setSystemClockService(m_systemClockService);
	// m_allocator.setActiveConsumerList(m_activeConsumerList);
	//
	// configureDefaultMockBehavior();
	// }
	//
	// @SuppressWarnings("unchecked")
	// private void configureDefaultMockBehavior() {
	// when(m_systemClockService.now()).thenReturn(0L);
	// when(m_config.getDefaultLeaseAcquireOrRenewRetryDelayMillis()).thenReturn(DEFAULT_LEASE_RETRY_DELAY);
	// when(m_config.getConsumerLeaseTimeMillis()).thenReturn(DEFAULT_LEASE_TIME);
	// when(m_config.getConsumerLeaseClientSideAdjustmentTimeMills()).thenReturn(DEFAULT_LEASE_ADJUST_TIME);
	// doAnswer(new Answer<Void>() {
	//
	// @Override
	// public Void answer(InvocationOnMock invocation) throws Throwable {
	// Object[] arguments = invocation.getArguments();
	// Pair<String, String> tg = (Pair<String, String>) arguments[0];
	// m_heartbeats.add(new HeartbeatInfo(tg.getKey(), tg.getValue(), (String) arguments[1], (String) arguments[2]));
	// return null;
	// }
	// }).when(m_activeConsumerList).heartbeat(any(Pair.class), anyString(), anyString());
	// }
	//
	// @Test
	// public void testAcquireLeaseWithoutAssignment() throws Exception {
	// String topic = "topic";
	// String group = "g1";
	// int partition = 1;
	// String consumerName = "c1";
	// String ip = "1.1.1.1";
	//
	// mockAssignmentHolderReturn(topic, group, null);
	//
	// LeaseAcquireResponse response = m_allocator.tryAcquireLease(new Tpg(topic, partition, group), consumerName, ip);
	// assertFalse(response.isAcquired());
	// assertNull(response.getLease());
	// assertEquals(DEFAULT_LEASE_RETRY_DELAY, response.getNextTryTime());
	// assertHeartbeat(Arrays.asList(new HeartbeatInfo(topic, group, consumerName, ip)));
	// }
	//
	// @Test
	// public void testRenewLeaseWithoutAssignment() throws Exception {
	// String topic = "topic";
	// String group = "g1";
	// int partition = 1;
	// String consumerName = "c1";
	// String ip = "1.1.1.1";
	//
	// mockAssignmentHolderReturn(topic, group, null);
	//
	// LeaseAcquireResponse response = m_allocator.tryRenewLease(new Tpg(topic, partition, group), consumerName, 1, ip);
	// assertFalse(response.isAcquired());
	// assertNull(response.getLease());
	// assertEquals(DEFAULT_LEASE_RETRY_DELAY, response.getNextTryTime());
	// assertHeartbeat(Arrays.asList(new HeartbeatInfo(topic, group, consumerName, ip)));
	// }
	//
	// @Test
	// public void testAcquireLeaseNotAssignToConsumerAndNoExistingLease() throws Exception {
	// String topic = "topic";
	// String group = "g1";
	// int partition = 1;
	// String consumerName = "c1";
	// String ip = "1.1.1.1";
	//
	// mockAssignmentHolderReturn(topic, group, new Assignment<Integer>());
	// mockLeaseHolderExecuteLeaseOperation(topic, partition, group, new HashMap<String, ClientLeaseInfo>());
	//
	// LeaseAcquireResponse response = m_allocator.tryAcquireLease(new Tpg(topic, partition, group), consumerName, ip);
	// assertFalse(response.isAcquired());
	// assertNull(response.getLease());
	// assertEquals(DEFAULT_LEASE_RETRY_DELAY, response.getNextTryTime());
	// assertHeartbeat(Arrays.asList(new HeartbeatInfo(topic, group, consumerName, ip)));
	// }
	//
	// @Test
	// public void testRenewLeaseNotAssignToConsumerAndNoExistingLease() throws Exception {
	// String topic = "topic";
	// String group = "g1";
	// int partition = 1;
	// String consumerName = "c1";
	// String ip = "1.1.1.1";
	//
	// mockAssignmentHolderReturn(topic, group, new Assignment<Integer>());
	// mockLeaseHolderExecuteLeaseOperation(topic, partition, group, new HashMap<String, ClientLeaseInfo>());
	//
	// LeaseAcquireResponse response = m_allocator.tryRenewLease(new Tpg(topic, partition, group), consumerName, 1, ip);
	// assertFalse(response.isAcquired());
	// assertNull(response.getLease());
	// assertEquals(DEFAULT_LEASE_RETRY_DELAY, response.getNextTryTime());
	// }
	//
	// @Test
	// public void testAcquireLeaseNotAssignToConsumerAndFoundExistingLease() throws Exception {
	// String topic = "topic";
	// String group = "g1";
	// int partition = 1;
	// String consumerName = "c1";
	// String ip = "1.1.1.1";
	//
	// Map<String, ClientLeaseInfo> existingValidLeases = new HashMap<String, ClientLeaseInfo>();
	// existingValidLeases.put("c2", new ClientLeaseInfo(new Lease(1, 100L), "1.1.1.2", 1234));
	//
	// mockAssignmentHolderReturn(topic, group, new Assignment<Integer>());
	// mockLeaseHolderExecuteLeaseOperation(topic, partition, group, existingValidLeases);
	//
	// LeaseAcquireResponse response = m_allocator.tryAcquireLease(new Tpg(topic, partition, group), consumerName, ip);
	// assertFalse(response.isAcquired());
	// assertNull(response.getLease());
	// assertEquals(100L, response.getNextTryTime());
	// assertHeartbeat(Arrays.asList(new HeartbeatInfo(topic, group, consumerName, ip)));
	// }
	//
	// @Test
	// public void testRenewLeaseNotAssignToConsumerAndFoundExistingLease() throws Exception {
	// String topic = "topic";
	// String group = "g1";
	// int partition = 1;
	// String consumerName = "c1";
	// String ip = "1.1.1.1";
	//
	// Map<String, ClientLeaseInfo> existingValidLeases = new HashMap<String, ClientLeaseInfo>();
	// existingValidLeases.put("c2", new ClientLeaseInfo(new Lease(1, 100L), "1.1.1.2", 1234));
	//
	// mockAssignmentHolderReturn(topic, group, new Assignment<Integer>());
	// mockLeaseHolderExecuteLeaseOperation(topic, partition, group, existingValidLeases);
	//
	// LeaseAcquireResponse response = m_allocator.tryRenewLease(new Tpg(topic, partition, group), consumerName, 1, ip);
	// assertFalse(response.isAcquired());
	// assertNull(response.getLease());
	// assertEquals(100L, response.getNextTryTime());
	// }
	//
	// @Test
	// public void testAcquireLeaseSuccessAndNoExistingLease() throws Exception {
	// String topic = "topic";
	// String group = "g1";
	// int partition = 1;
	// String consumerName = "c1";
	// String ip = "1.1.1.1";
	//
	// Assignment<Integer> assignment = new Assignment<Integer>();
	// Map<String, ClientContext> consumers = new HashMap<>();
	// ClientContext consumer = new ClientContext(consumerName, "1.1.1.1", 1234, null, null, 1L);
	// consumers.put(consumerName, consumer);
	// assignment.addAssignment(partition, consumers);
	//
	// mockAssignmentHolderReturn(topic, group, assignment);
	// mockLeaseHolderExecuteLeaseOperation(topic, partition, group, new HashMap<String, ClientLeaseInfo>());
	// mockLeaseHolderNewLease(topic, partition, group, consumerName, 1, DEFAULT_LEASE_TIME);
	//
	// LeaseAcquireResponse response = m_allocator.tryAcquireLease(new Tpg(topic, partition, group), consumerName, ip);
	//
	// assertTrue(response.isAcquired());
	// Lease lease = response.getLease();
	// assertNotNull(lease);
	// assertEquals(1, lease.getId());
	// assertEquals(DEFAULT_LEASE_TIME + DEFAULT_LEASE_ADJUST_TIME, lease.getExpireTime());
	// assertEquals(-1L, response.getNextTryTime());
	// assertHeartbeat(Arrays.asList(new HeartbeatInfo(topic, group, consumerName, ip)));
	// }
	//
	// @Test
	// public void testRenewLeaseFailSinceNoExistingLease() throws Exception {
	// String topic = "topic";
	// String group = "g1";
	// int partition = 1;
	// String consumerName = "c1";
	// String ip = "1.1.1.1";
	//
	// Assignment<Integer> assignment = new Assignment<Integer>();
	// Map<String, ClientContext> consumers = new HashMap<>();
	// ClientContext consumer = new ClientContext(consumerName, "1.1.1.1", 1234, null, null, 1L);
	// consumers.put(consumerName, consumer);
	// assignment.addAssignment(partition, consumers);
	//
	// mockAssignmentHolderReturn(topic, group, assignment);
	// mockLeaseHolderExecuteLeaseOperation(topic, partition, group, new HashMap<String, ClientLeaseInfo>());
	//
	// LeaseAcquireResponse response = m_allocator.tryRenewLease(new Tpg(topic, partition, group), consumerName, 1, ip);
	//
	// assertFalse(response.isAcquired());
	// assertNull(response.getLease());
	// assertEquals(DEFAULT_LEASE_RETRY_DELAY, response.getNextTryTime());
	// }
	//
	// @Test
	// public void testAcquireLeaseFailSinceAnotherConsumerHoldingLease() throws Exception {
	// String topic = "topic";
	// String group = "g1";
	// int partition = 1;
	// String consumerName = "c1";
	// String ip = "1.1.1.1";
	//
	// Assignment<Integer> assignment = new Assignment<Integer>();
	// Map<String, ClientContext> consumers = new HashMap<>();
	// ClientContext consumer = new ClientContext(consumerName, ip, -1, null, null, 1L);
	// consumers.put(consumerName, consumer);
	// assignment.addAssignment(partition, consumers);
	//
	// mockAssignmentHolderReturn(topic, group, assignment);
	// HashMap<String, ClientLeaseInfo> existingValidLeases = new HashMap<String, ClientLeaseInfo>();
	// existingValidLeases.put(consumerName + "2", new ClientLeaseInfo(new Lease(1, 100L), "1.1.1.2", 1234));
	// mockLeaseHolderExecuteLeaseOperation(topic, partition, group, existingValidLeases);
	// mockLeaseHolderNewLease(topic, partition, group, consumerName, 1, DEFAULT_LEASE_TIME);
	//
	// LeaseAcquireResponse response = m_allocator.tryAcquireLease(new Tpg(topic, partition, group), consumerName, ip);
	//
	// assertFalse(response.isAcquired());
	// assertNull(response.getLease());
	// assertEquals(100L, response.getNextTryTime());
	// assertHeartbeat(Arrays.asList(new HeartbeatInfo(topic, group, consumerName, ip)));
	// }
	//
	// @Test
	// public void testRenewLeaseFailSinceAnotherConsumerHoldingLease() throws Exception {
	// String topic = "topic";
	// String group = "g1";
	// int partition = 1;
	// String consumerName = "c1";
	// String ip = "1.1.1.1";
	//
	// Assignment<Integer> assignment = new Assignment<Integer>();
	// Map<String, ClientContext> consumers = new HashMap<>();
	// ClientContext consumer = new ClientContext(consumerName, "1.1.1.1", 1234, null, null, 1L);
	// consumers.put(consumerName, consumer);
	// assignment.addAssignment(partition, consumers);
	//
	// mockAssignmentHolderReturn(topic, group, assignment);
	// HashMap<String, ClientLeaseInfo> existingValidLeases = new HashMap<String, ClientLeaseInfo>();
	// existingValidLeases.put("c2", new ClientLeaseInfo(new Lease(1, 100L), "1.1.1.2", 1234));
	// mockLeaseHolderExecuteLeaseOperation(topic, partition, group, existingValidLeases);
	//
	// LeaseAcquireResponse response = m_allocator.tryRenewLease(new Tpg(topic, partition, group), consumerName, 1, ip);
	//
	// assertFalse(response.isAcquired());
	// assertNull(response.getLease());
	// assertEquals(100L, response.getNextTryTime());
	// }
	//
	// @Test
	// public void testAcquireLeaseSucessAndConsumerAlreadyHoldLease() throws Exception {
	// String topic = "topic";
	// String group = "g1";
	// int partition = 1;
	// String consumerName = "c1";
	// String ip = "1.1.1.1";
	//
	// Assignment<Integer> assignment = new Assignment<Integer>();
	// Map<String, ClientContext> consumers = new HashMap<>();
	// ClientContext consumer = new ClientContext(consumerName, "1.1.1.1", 1234, null, null, 1L);
	// consumers.put(consumerName, consumer);
	// assignment.addAssignment(partition, consumers);
	//
	// mockAssignmentHolderReturn(topic, group, assignment);
	// HashMap<String, ClientLeaseInfo> existingValidLeases = new HashMap<String, ClientLeaseInfo>();
	// existingValidLeases.put(consumerName, new ClientLeaseInfo(new Lease(1, 10000), "1.1.1.1", 1234));
	// mockLeaseHolderExecuteLeaseOperation(topic, partition, group, existingValidLeases);
	//
	// LeaseAcquireResponse response = m_allocator.tryAcquireLease(new Tpg(topic, partition, group), consumerName, ip);
	//
	// assertTrue(response.isAcquired());
	// Lease lease = response.getLease();
	// assertNotNull(lease);
	// assertEquals(1, lease.getId());
	// assertEquals(10000 + DEFAULT_LEASE_ADJUST_TIME, lease.getExpireTime());
	// assertEquals(-1, response.getNextTryTime());
	// assertHeartbeat(Arrays.asList(new HeartbeatInfo(topic, group, consumerName, ip)));
	// }
	//
	// @Test
	// public void testRenewLeaseSucess() throws Exception {
	// String topic = "topic";
	// String group = "g1";
	// int partition = 1;
	// String consumerName = "c1";
	// String ip = "1.1.1.1";
	//
	// Assignment<Integer> assignment = new Assignment<Integer>();
	// Map<String, ClientContext> consumers = new HashMap<>();
	// ClientContext consumer = new ClientContext(consumerName, "1.1.1.1", 1234, null, null, 1L);
	// consumers.put(consumerName, consumer);
	// assignment.addAssignment(partition, consumers);
	//
	// mockAssignmentHolderReturn(topic, group, assignment);
	// HashMap<String, ClientLeaseInfo> existingValidLeases = new HashMap<String, ClientLeaseInfo>();
	// existingValidLeases.put(consumerName, new ClientLeaseInfo(new Lease(1, 10000), "1.1.1.1", 1234));
	// mockLeaseHolderRenewLease(topic, partition, group, consumerName, "1.1.1.1", 1234, 1L, DEFAULT_LEASE_TIME);
	// mockLeaseHolderExecuteLeaseOperation(topic, partition, group, existingValidLeases);
	//
	// LeaseAcquireResponse response = m_allocator.tryRenewLease(new Tpg(topic, partition, group), consumerName, 1, ip);
	//
	// assertTrue(response.isAcquired());
	// Lease lease = response.getLease();
	// assertNotNull(lease);
	// assertEquals(1, lease.getId());
	// assertEquals(10000 + DEFAULT_LEASE_TIME + DEFAULT_LEASE_ADJUST_TIME, lease.getExpireTime());
	// assertEquals(-1, response.getNextTryTime());
	// }
	//
	// @Test
	// public void testRenewLeaseFailSinceExistingLeaseIdNotMatch() throws Exception {
	// String topic = "topic";
	// String group = "g1";
	// int partition = 1;
	// String consumerName = "c1";
	// String ip = "1.1.1.1";
	//
	// Assignment<Integer> assignment = new Assignment<Integer>();
	// Map<String, ClientContext> consumers = new HashMap<>();
	// ClientContext consumer = new ClientContext(consumerName, "1.1.1.1", 1234, null, null, 1L);
	// consumers.put(consumerName, consumer);
	// assignment.addAssignment(partition, consumers);
	//
	// mockAssignmentHolderReturn(topic, group, assignment);
	// HashMap<String, ClientLeaseInfo> existingValidLeases = new HashMap<String, ClientLeaseInfo>();
	// existingValidLeases.put(consumerName, new ClientLeaseInfo(new Lease(2, 10000), "1.1.1.1", 1234));
	// mockLeaseHolderExecuteLeaseOperation(topic, partition, group, existingValidLeases);
	//
	// LeaseAcquireResponse response = m_allocator.tryRenewLease(new Tpg(topic, partition, group), consumerName, 1, ip);
	//
	// assertFalse(response.isAcquired());
	// assertNull(response.getLease());
	// assertEquals(10000, response.getNextTryTime());
	// }
	//
	// @SuppressWarnings("unchecked")
	// private void mockLeaseHolderNewLease(String topic, int partition, String group, String consumerName, long leaseId,
	// long leaseTime) throws Exception {
	// when(
	// m_leaseHolder.newLease(eq(new Tpg(topic, partition, group)), eq(consumerName), anyMap(), eq(leaseTime),
	// anyString(), anyInt())).thenReturn(new Lease(leaseId, leaseTime));
	// }
	//
	// @SuppressWarnings("unchecked")
	// private void mockLeaseHolderRenewLease(String topic, int partition, String group, String consumerName,
	// final String ip, final int port, long leaseId, final long leaseTime) throws Exception {
	// doAnswer(new Answer<Void>() {
	//
	// @Override
	// public Void answer(InvocationOnMock invocation) throws Throwable {
	// ClientLeaseInfo clientLeaseInfo = invocation.getArgumentAt(3, ClientLeaseInfo.class);
	// clientLeaseInfo.getLease().setExpireTime(clientLeaseInfo.getLease().getExpireTime() + leaseTime);
	// clientLeaseInfo.setIp(ip);
	// clientLeaseInfo.setPort(port);
	//
	// return null;
	// }
	// }).when(m_leaseHolder).renewLease(eq(new Tpg(topic, partition, group)), eq(consumerName), anyMap(),
	// any(ClientLeaseInfo.class), eq(leaseTime), anyString(), anyInt());
	//
	// }
	//
	// private void mockLeaseHolderExecuteLeaseOperation(String topic, int partition, String group,
	// final Map<String, ClientLeaseInfo> existingValidLeases) throws Exception {
	// when(m_leaseHolder.executeLeaseOperation(eq(new Tpg(topic, partition, group)), any(LeaseOperationCallback.class)))
	// .thenAnswer(new Answer<LeaseAcquireResponse>() {
	//
	// @Override
	// public LeaseAcquireResponse answer(InvocationOnMock invocation) throws Throwable {
	// LeaseOperationCallback callback = invocation.getArgumentAt(1, LeaseOperationCallback.class);
	//
	// return callback.execute(existingValidLeases);
	// }
	// });
	// }
	//
	// private void mockAssignmentHolderReturn(String topic, String group, Assignment<Integer> assignment) {
	// when(m_assignmentHolder.getAssignment(eq(new Pair<String, String>(topic, group)))).thenReturn(assignment);
	// }
	//
	// private void assertHeartbeat(List<HeartbeatInfo> expectedHeartbeats) {
	// assertEquals(expectedHeartbeats, m_heartbeats);
	// }
}
