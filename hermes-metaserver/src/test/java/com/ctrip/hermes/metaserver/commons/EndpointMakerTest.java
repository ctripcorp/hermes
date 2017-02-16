package com.ctrip.hermes.metaserver.commons;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.metaserver.broker.BrokerLeaseHolder;
import com.ctrip.hermes.metaserver.config.MetaServerConfig;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class EndpointMakerTest {
	@Mock
	private BrokerLeaseHolder m_brokerLeaseHolder;

	@Mock
	private MetaServerConfig m_config;

	@Mock
	private ScheduledExecutorService m_scheduledExecutor;

	private EndpointMaker m_maker;

	@Before
	public void before() throws Exception {
		m_maker = new EndpointMaker();
		m_maker.setBrokerLeaseHolder(m_brokerLeaseHolder);
		m_maker.setConfig(m_config);
		m_maker.setScheduledExecutor(m_scheduledExecutor);
	}

	@Test
	public void test() throws Exception {
		long now = System.currentTimeMillis();

		Map<String, Assignment<Integer>> brokerAssignments = new HashMap<>();

		Assignment<Integer> t1Assignment = new Assignment<Integer>();
		t1Assignment.addAssignment(1, createBroker("br1", "1.1.1.1", 1111));
		t1Assignment.addAssignment(2, createBroker("br2", "1.1.1.2", 2222));
		t1Assignment.addAssignment(3, createBroker("br1", "1.1.1.1", 1111));
		brokerAssignments.put("t1", t1Assignment);

		Assignment<Integer> t2Assignment = new Assignment<Integer>();
		t2Assignment.addAssignment(1, createBroker("br1", "1.1.1.1", 1111));
		t2Assignment.addAssignment(2, createBroker("br2", "1.1.1.2", 2222));
		t2Assignment.addAssignment(3, createBroker("br3", "1.1.1.3", 3333));
		brokerAssignments.put("t2", t2Assignment);

		Map<Pair<String, Integer>, Map<String, ClientLeaseInfo>> leases = new HashMap<>();
		leases.put(new Pair<String, Integer>("t1", 2), createBrokerLease("br1", "1.1.1.11", 8888, 1, now + 10 * 1000L));
		leases.put(new Pair<String, Integer>("t1", 4), createBrokerLease("br1", "1.1.1.11", 8888, 1, now + 10 * 1000L));
		leases.put(new Pair<String, Integer>("t2", 1), createBrokerLease("br1", "1.1.1.1", 1111, 2, now + 20 * 1000L));

		when(m_brokerLeaseHolder.getAllValidLeases()).thenReturn(leases);
		when(m_scheduledExecutor.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS))).thenReturn(null);

		Map<String, Map<Integer, Endpoint>> endpoints = m_maker.makeEndpoints(null, -1, null, brokerAssignments, false);

		verify(m_scheduledExecutor, times(1)).schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS));

		assertEquals(2, endpoints.size());
		Map<Integer, Endpoint> endpoints1 = endpoints.get("t1");
		assertEquals(3, endpoints1.size());

		assertEndpoint("br1", "1.1.1.1", 1111, endpoints1.get(1));
		assertEndpoint("br1", "1.1.1.11", 8888, endpoints1.get(2));
		assertEndpoint("br1", "1.1.1.1", 1111, endpoints1.get(3));

		Map<Integer, Endpoint> endpoints2 = endpoints.get("t2");
		assertEquals(3, endpoints2.size());

		assertEndpoint("br1", "1.1.1.1", 1111, endpoints2.get(1));
		assertEndpoint("br2", "1.1.1.2", 2222, endpoints2.get(2));
		assertEndpoint("br3", "1.1.1.3", 3333, endpoints2.get(3));
	}

	private void assertEndpoint(String id, String host, int port, Endpoint actual) {
		assertEquals(id, actual.getId());
		assertEquals(host, actual.getHost());
		assertEquals(port, actual.getPort().intValue());
		assertEquals(Endpoint.BROKER, actual.getType());
	}

	private Map<String, ClientContext> createBroker(String name, String ip, int port) {
		Map<String, ClientContext> res = new HashMap<>();
		res.put(name, new ClientContext(name, ip, port, null, null, -1));
		return res;
	}

	private Map<String, ClientLeaseInfo> createBrokerLease(String brokerName, String ip, int port, long leaseId,
	      long leasExpireTime) {
		Map<String, ClientLeaseInfo> res = new HashMap<>();

		res.put(brokerName, new ClientLeaseInfo(new Lease(leaseId, leasExpireTime), ip, port));
		return res;
	}

}
