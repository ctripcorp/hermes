package com.ctrip.hermes.metaserver.consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.ctrip.hermes.metaserver.TestHelper;
import com.ctrip.hermes.metaserver.commons.ClientContext;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class ActiveConsumerListTest {

	private ActiveConsumerList m_list;

	@Before
	public void before() throws Exception {
		m_list = new ActiveConsumerList();
	}

	@Test
	public void testHeartbeat() throws Exception {
		String consumerName = "c1";
		String ip = "1.1.1.1";
		long heartbeatTime = 1L;
		m_list.heartbeat(consumerName, heartbeatTime, ip);

		Map<String, ClientContext> activeConsumers = m_list.getActiveConsumers();
		assertEquals(1, activeConsumers.size());
		TestHelper.assertClientContextEquals(consumerName, ip, heartbeatTime, activeConsumers.get(consumerName));
		assertTrue(m_list.getAndResetChanged());
	}

	@Test
	public void testHeartbeatTwiceWithoutChange() throws Exception {
		String consumerName = "c1";
		String ip = "1.1.1.1";
		long heartbeatTime = 1L;
		m_list.heartbeat(consumerName, heartbeatTime, ip);
		m_list.getAndResetChanged();
		m_list.heartbeat(consumerName, heartbeatTime + 1, ip);

		Map<String, ClientContext> activeConsumers = m_list.getActiveConsumers();
		assertEquals(1, activeConsumers.size());
		TestHelper.assertClientContextEquals(consumerName, ip, heartbeatTime + 1, activeConsumers.get(consumerName));
		assertFalse(m_list.getAndResetChanged());

	}

	@Test
	public void testHeartbeatTwiceWithChange() throws Exception {
		String consumerName = "c1";
		String ip = "1.1.1.1";
		long heartbeatTime = 1L;
		m_list.heartbeat(consumerName, heartbeatTime, ip);
		m_list.getAndResetChanged();
		m_list.heartbeat(consumerName, heartbeatTime + 1, ip + "2");

		Map<String, ClientContext> activeConsumers = m_list.getActiveConsumers();
		assertEquals(1, activeConsumers.size());
		TestHelper
		      .assertClientContextEquals(consumerName, ip + "2", heartbeatTime + 1, activeConsumers.get(consumerName));
		assertTrue(m_list.getAndResetChanged());

	}

	@Test
	public void testPurgeExpired() throws Exception {
		String consumerName = "c1";
		String ip = "1.1.1.1";
		long heartbeatTime = 1L;
		m_list.heartbeat(consumerName, heartbeatTime, ip);
		m_list.purgeExpired(10, 12L);

		Map<String, ClientContext> activeConsumers = m_list.getActiveConsumers();
		assertEquals(0, activeConsumers.size());
		assertTrue(m_list.getAndResetChanged());

		m_list.heartbeat(consumerName, heartbeatTime, ip);
		m_list.purgeExpired(10, 1L);
		activeConsumers = m_list.getActiveConsumers();
		assertEquals(1, activeConsumers.size());
		TestHelper.assertClientContextEquals(consumerName, ip, heartbeatTime, activeConsumers.get(consumerName));
		assertTrue(m_list.getAndResetChanged());
	}

}
