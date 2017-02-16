package com.ctrip.hermes.metaserver.broker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.metaserver.commons.Assignment;
import com.ctrip.hermes.metaserver.commons.ClientContext;
import com.ctrip.hermes.metaserver.commons.ClientLeaseInfo;
import com.ctrip.hermes.metaserver.commons.LeaseHolder.LeaseOperationCallback;
import com.ctrip.hermes.metaserver.commons.LeaseHolder.LeasesContext;
import com.ctrip.hermes.metaserver.config.MetaServerConfig;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultBrokerLeaseAllocatorTest {
//	private static final long DEFAULT_LEASE_RETRY_DELAY = 1000L;
//
//	private static final long DEFAULT_LEASE_TIME = 5000L;
//
//	private static final long DEFAULT_LEASE_ADJUST_TIME = -123L;
//
//	@Mock
//	private MetaServerConfig m_config;
//
//	@Mock
//	private SystemClockService m_systemClockService;
//
//	@Mock
//	private BrokerLeaseHolder m_leaseHolder;
//
//	@Mock
//	private BrokerAssignmentHolder m_assignmentHolder;
//
//	private DefaultBrokerLeaseAllocator m_allocator;
//
//	@Before
//	public void before() throws Exception {
//		m_allocator = new DefaultBrokerLeaseAllocator();
//		m_allocator.setAssignmentHolder(m_assignmentHolder);
//		m_allocator.setConfig(m_config);
//		m_allocator.setLeaseHolder(m_leaseHolder);
//		m_allocator.setSystemClockService(m_systemClockService);
//
//		configureDefaultMockBehavior();
//	}
//
//	private void configureDefaultMockBehavior() {
//		when(m_systemClockService.now()).thenReturn(0L);
//		when(m_config.getDefaultLeaseAcquireOrRenewRetryDelayMillis()).thenReturn(DEFAULT_LEASE_RETRY_DELAY);
//		when(m_config.getBrokerLeaseTimeMillis()).thenReturn(DEFAULT_LEASE_TIME);
//		when(m_config.getBrokerLeaseClientSideAdjustmentTimeMills()).thenReturn(DEFAULT_LEASE_ADJUST_TIME);
//	}
//
//	@Test
//	public void testAcquireLeaseWithoutAssignment() throws Exception {
//		String topic = "topic";
//
//		mockAssignmentHolderReturn(topic, null);
//
//		LeaseAcquireResponse response = m_allocator.tryAcquireLease(topic, 1, "br0", "0.0.0.0", 1234);
//		assertFalse(response.isAcquired());
//		assertNull(response.getLease());
//		assertEquals(DEFAULT_LEASE_RETRY_DELAY, response.getNextTryTime());
//	}
//
//	@Test
//	public void testRenewLeaseWithoutAssignment() throws Exception {
//		String topic = "topic";
//
//		mockAssignmentHolderReturn(topic, null);
//
//		LeaseAcquireResponse response = m_allocator.tryRenewLease(topic, 1, "br0", 1, "0.0.0.0", 1234);
//		assertFalse(response.isAcquired());
//		assertNull(response.getLease());
//		assertEquals(DEFAULT_LEASE_RETRY_DELAY, response.getNextTryTime());
//	}
//
//	@Test
//	public void testAcquireLeaseNotAssignToBrokerAndNoExistingLease() throws Exception {
//		String topic = "topic";
//		int partition = 1;
//
//		mockAssignmentHolderReturn(topic, new Assignment<Integer>());
//		mockLeaseHolderExecuteLeaseOperation(topic, partition, new HashMap<String, ClientLeaseInfo>());
//
//		LeaseAcquireResponse response = m_allocator.tryAcquireLease(topic, partition, "br0", "1.1.1.1", 1234);
//		assertFalse(response.isAcquired());
//		assertNull(response.getLease());
//		assertEquals(DEFAULT_LEASE_RETRY_DELAY, response.getNextTryTime());
//	}
//
//	@Test
//	public void testRenewLeaseNotAssignToBrokerAndNoExistingLease() throws Exception {
//		String topic = "topic";
//		int partition = 1;
//
//		mockAssignmentHolderReturn(topic, new Assignment<Integer>());
//		mockLeaseHolderExecuteLeaseOperation(topic, partition, new HashMap<String, ClientLeaseInfo>());
//
//		LeaseAcquireResponse response = m_allocator.tryRenewLease(topic, partition, "br0", 1, "1.1.1.1", 1234);
//		assertFalse(response.isAcquired());
//		assertNull(response.getLease());
//		assertEquals(DEFAULT_LEASE_RETRY_DELAY, response.getNextTryTime());
//	}
//
//	@Test
//	public void testAcquireLeaseNotAssignToBrokerAndFoundExistingLease() throws Exception {
//		String topic = "topic";
//		int partition = 1;
//		Map<String, ClientLeaseInfo> existingValidLeases = new HashMap<String, ClientLeaseInfo>();
//		existingValidLeases.put("br1", new ClientLeaseInfo(new Lease(1, 100L), "1.1.1.2", 1234));
//
//		mockAssignmentHolderReturn(topic, new Assignment<Integer>());
//		mockLeaseHolderExecuteLeaseOperation(topic, partition, existingValidLeases);
//
//		LeaseAcquireResponse response = m_allocator.tryAcquireLease(topic, partition, "br0", "1.1.1.1", 1234);
//		assertFalse(response.isAcquired());
//		assertNull(response.getLease());
//		assertEquals(100L, response.getNextTryTime());
//	}
//
//	@Test
//	public void testRenewLeaseNotAssignToBrokerAndFoundExistingLease() throws Exception {
//		String topic = "topic";
//		int partition = 1;
//		Map<String, ClientLeaseInfo> existingValidLeases = new HashMap<String, ClientLeaseInfo>();
//		existingValidLeases.put("br1", new ClientLeaseInfo(new Lease(1, 100L), "1.1.1.2", 1234));
//
//		mockAssignmentHolderReturn(topic, new Assignment<Integer>());
//		mockLeaseHolderExecuteLeaseOperation(topic, partition, existingValidLeases);
//
//		LeaseAcquireResponse response = m_allocator.tryRenewLease(topic, partition, "br0", 1, "1.1.1.1", 1234);
//		assertFalse(response.isAcquired());
//		assertNull(response.getLease());
//		assertEquals(100L, response.getNextTryTime());
//	}
//
//	@Test
//	public void testAcquireLeaseSuccessAndNoExistingLease() throws Exception {
//		String topic = "topic";
//		int partition = 1;
//		String brokerName = "br0";
//
//		Assignment<Integer> assignment = new Assignment<Integer>();
//		Map<String, ClientContext> brokers = new HashMap<>();
//		ClientContext broker = new ClientContext(brokerName, "1.1.1.1", 1234, null, null, 1L);
//		brokers.put(brokerName, broker);
//		assignment.addAssignment(partition, brokers);
//
//		mockAssignmentHolderReturn(topic, assignment);
//		mockLeaseHolderExecuteLeaseOperation(topic, partition, new HashMap<String, ClientLeaseInfo>());
//		mockLeaseHolderNewLease(topic, partition, brokerName, 1, DEFAULT_LEASE_TIME);
//
//		LeaseAcquireResponse response = m_allocator.tryAcquireLease(topic, partition, broker.getName(), broker.getIp(),
//		      broker.getPort());
//
//		assertTrue(response.isAcquired());
//		Lease lease = response.getLease();
//		assertNotNull(lease);
//		assertEquals(1, lease.getId());
//		assertEquals(DEFAULT_LEASE_TIME + DEFAULT_LEASE_ADJUST_TIME, lease.getExpireTime());
//		assertEquals(-1L, response.getNextTryTime());
//	}
//
//	@Test
//	public void testRenewLeaseFailSinceNoExistingLease() throws Exception {
//		String topic = "topic";
//		int partition = 1;
//		String brokerName = "br0";
//
//		Assignment<Integer> assignment = new Assignment<Integer>();
//		Map<String, ClientContext> brokers = new HashMap<>();
//		ClientContext broker = new ClientContext(brokerName, "1.1.1.1", 1234, null, null, 1L);
//		brokers.put(brokerName, broker);
//		assignment.addAssignment(partition, brokers);
//
//		mockAssignmentHolderReturn(topic, assignment);
//		mockLeaseHolderExecuteLeaseOperation(topic, partition, new HashMap<String, ClientLeaseInfo>());
//
//		LeaseAcquireResponse response = m_allocator.tryRenewLease(topic, partition, broker.getName(), 1, broker.getIp(),
//		      broker.getPort());
//
//		assertFalse(response.isAcquired());
//		assertNull(response.getLease());
//		assertEquals(DEFAULT_LEASE_RETRY_DELAY, response.getNextTryTime());
//	}
//
//	@Test
//	public void testAcquireLeaseButAnotherBrokerHoldingLease() throws Exception {
//		String topic = "topic";
//		int partition = 1;
//		String brokerName = "br0";
//
//		Assignment<Integer> assignment = new Assignment<Integer>();
//		Map<String, ClientContext> brokers = new HashMap<>();
//		ClientContext broker = new ClientContext(brokerName, "1.1.1.1", 1234, null, null, 1L);
//		brokers.put(brokerName, broker);
//		assignment.addAssignment(partition, brokers);
//
//		mockAssignmentHolderReturn(topic, assignment);
//		HashMap<String, ClientLeaseInfo> existingValidLeases = new HashMap<String, ClientLeaseInfo>();
//		existingValidLeases.put("br1", new ClientLeaseInfo(new Lease(1, 100L), "1.1.1.2", 1234));
//		mockLeaseHolderExecuteLeaseOperation(topic, partition, existingValidLeases);
//
//		LeaseAcquireResponse response = m_allocator.tryAcquireLease(topic, partition, broker.getName(), broker.getIp(),
//		      broker.getPort());
//
//		assertFalse(response.isAcquired());
//		assertNull(response.getLease());
//		assertEquals(100L, response.getNextTryTime());
//	}
//
//	@Test
//	public void testRenewLeaseFailSinceAnotherBrokerHoldingLease() throws Exception {
//		String topic = "topic";
//		int partition = 1;
//		String brokerName = "br0";
//
//		Assignment<Integer> assignment = new Assignment<Integer>();
//		Map<String, ClientContext> brokers = new HashMap<>();
//		ClientContext broker = new ClientContext(brokerName, "1.1.1.1", 1234, null, null, 1L);
//		brokers.put(brokerName, broker);
//		assignment.addAssignment(partition, brokers);
//
//		mockAssignmentHolderReturn(topic, assignment);
//		HashMap<String, ClientLeaseInfo> existingValidLeases = new HashMap<String, ClientLeaseInfo>();
//		existingValidLeases.put("br1", new ClientLeaseInfo(new Lease(1, 100L), "1.1.1.2", 1234));
//		mockLeaseHolderExecuteLeaseOperation(topic, partition, existingValidLeases);
//
//		LeaseAcquireResponse response = m_allocator.tryRenewLease(topic, partition, broker.getName(), 1, broker.getIp(),
//		      broker.getPort());
//
//		assertFalse(response.isAcquired());
//		assertNull(response.getLease());
//		assertEquals(100L, response.getNextTryTime());
//	}
//
//	@Test
//	public void testAcquireLeaseSucessAndBrokerAlreadyHoldLease() throws Exception {
//		String topic = "topic";
//		int partition = 1;
//		String brokerName = "br0";
//
//		Assignment<Integer> assignment = new Assignment<Integer>();
//		Map<String, ClientContext> brokers = new HashMap<>();
//		ClientContext broker = new ClientContext(brokerName, "1.1.1.1", 1234, null, null, 1L);
//		brokers.put(brokerName, broker);
//		assignment.addAssignment(partition, brokers);
//
//		mockAssignmentHolderReturn(topic, assignment);
//		HashMap<String, ClientLeaseInfo> existingValidLeases = new HashMap<String, ClientLeaseInfo>();
//		existingValidLeases.put(brokerName, new ClientLeaseInfo(new Lease(1, 10000), "1.1.1.1", 1234));
//		mockLeaseHolderExecuteLeaseOperation(topic, partition, existingValidLeases);
//
//		LeaseAcquireResponse response = m_allocator.tryAcquireLease(topic, partition, broker.getName(), broker.getIp(),
//		      broker.getPort());
//
//		assertTrue(response.isAcquired());
//		Lease lease = response.getLease();
//		assertNotNull(lease);
//		assertEquals(1, lease.getId());
//		assertEquals(10000 + DEFAULT_LEASE_ADJUST_TIME, lease.getExpireTime());
//		assertEquals(-1, response.getNextTryTime());
//	}
//
//	@Test
//	public void testRenewLeaseSucess() throws Exception {
//		String topic = "topic";
//		int partition = 1;
//		String brokerName = "br0";
//
//		Assignment<Integer> assignment = new Assignment<Integer>();
//		Map<String, ClientContext> brokers = new HashMap<>();
//		ClientContext broker = new ClientContext(brokerName, "1.1.1.1", 1234, null, null, 1L);
//		brokers.put(brokerName, broker);
//		assignment.addAssignment(partition, brokers);
//
//		mockAssignmentHolderReturn(topic, assignment);
//		HashMap<String, ClientLeaseInfo> existingValidLeases = new HashMap<String, ClientLeaseInfo>();
//		existingValidLeases.put(brokerName, new ClientLeaseInfo(new Lease(1, 10000), "1.1.1.1", 1234));
//		mockLeaseHolderRenewLease(topic, partition, brokerName, "1.1.1.1", 1234, 1, DEFAULT_LEASE_TIME);
//		mockLeaseHolderExecuteLeaseOperation(topic, partition, existingValidLeases);
//
//		LeaseAcquireResponse response = m_allocator.tryRenewLease(topic, partition, broker.getName(), 1, broker.getIp(),
//		      broker.getPort());
//
//		assertTrue(response.isAcquired());
//		Lease lease = response.getLease();
//		assertNotNull(lease);
//		assertEquals(1, lease.getId());
//		assertEquals(10000 + DEFAULT_LEASE_TIME + DEFAULT_LEASE_ADJUST_TIME, lease.getExpireTime());
//		assertEquals(-1, response.getNextTryTime());
//	}
//
//	@Test
//	public void testRenewLeaseFailSinceExistingLeaseIdNotMatch() throws Exception {
//		String topic = "topic";
//		int partition = 1;
//		String brokerName = "br0";
//
//		Assignment<Integer> assignment = new Assignment<Integer>();
//		Map<String, ClientContext> brokers = new HashMap<>();
//		ClientContext broker = new ClientContext(brokerName, "1.1.1.1", 1234, null, null, 1L);
//		brokers.put(brokerName, broker);
//		assignment.addAssignment(partition, brokers);
//
//		mockAssignmentHolderReturn(topic, assignment);
//		HashMap<String, ClientLeaseInfo> existingValidLeases = new HashMap<String, ClientLeaseInfo>();
//		existingValidLeases.put(brokerName, new ClientLeaseInfo(new Lease(1, 10000), "1.1.1.1", 1234));
//		mockLeaseHolderRenewLease(topic, partition, brokerName, "2.2.2.2", 4321, 1, DEFAULT_LEASE_TIME);
//		mockLeaseHolderExecuteLeaseOperation(topic, partition, existingValidLeases);
//
//		LeaseAcquireResponse response = m_allocator.tryRenewLease(topic, partition, broker.getName(), 3, broker.getIp(),
//		      broker.getPort());
//
//		assertFalse(response.isAcquired());
//		assertNull(response.getLease());
//		assertEquals(10000, response.getNextTryTime());
//	}
//
//	@SuppressWarnings("unchecked")
//	private void mockLeaseHolderNewLease(String topic, int partition, String broker, long leaseId, long leaseTime)
//	      throws Exception {
//		when(
//		      m_leaseHolder.newLease(eq(new Pair<String, Integer>(topic, partition)), eq(broker),
//		            any(LeasesContext.class), eq(leaseTime), anyString(), anyInt())).thenReturn(
//		      new Lease(leaseId, leaseTime));
//	}
//
//	@SuppressWarnings("unchecked")
//	private void mockLeaseHolderRenewLease(String topic, int partition, String broker, final String ip, final int port,
//	      long leaseId, final long leaseTime) throws Exception {
//		doAnswer(new Answer<Void>() {
//
//			@Override
//			public Void answer(InvocationOnMock invocation) throws Throwable {
//				ClientLeaseInfo clientLeaseInfo = invocation.getArgumentAt(3, ClientLeaseInfo.class);
//				clientLeaseInfo.getLease().setExpireTime(clientLeaseInfo.getLease().getExpireTime() + leaseTime);
//				clientLeaseInfo.setIp(ip);
//				clientLeaseInfo.setPort(port);
//
//				return null;
//			}
//		}).when(m_leaseHolder).renewLease(eq(new Pair<String, Integer>(topic, partition)), eq(broker),
//		      any(LeasesContext.class), any(ClientLeaseInfo.class), eq(leaseTime), anyString(), anyInt());
//
//	}
//
//	private void mockLeaseHolderExecuteLeaseOperation(String topic, int partition,
//	      final Map<String, ClientLeaseInfo> existingValidLeases) throws Exception {
//		when(
//		      m_leaseHolder.executeLeaseOperation(eq(new Pair<String, Integer>(topic, partition)),
//		            any(LeaseOperationCallback.class))).thenAnswer(new Answer<LeaseAcquireResponse>() {
//
//			@Override
//			public LeaseAcquireResponse answer(InvocationOnMock invocation) throws Throwable {
//				LeaseOperationCallback callback = invocation.getArgumentAt(1, LeaseOperationCallback.class);
//
//				return callback.execute(existingValidLeases);
//			}
//		});
//	}
//
//	private void mockAssignmentHolderReturn(String topic, Assignment<Integer> assignment) {
//		when(m_assignmentHolder.getAssignment(eq(topic))).thenReturn(assignment);
//	}

}
