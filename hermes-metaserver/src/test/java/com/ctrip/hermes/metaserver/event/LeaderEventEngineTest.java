package com.ctrip.hermes.metaserver.event;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.curator.utils.ZKPaths;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.bo.HostPort;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Idc;
import com.ctrip.hermes.meta.entity.Server;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaserver.TestHelper;
import com.ctrip.hermes.metaserver.ZKSuppportTestCase;
import com.ctrip.hermes.metaserver.broker.BrokerAssignmentHolder;
import com.ctrip.hermes.metaserver.cluster.ClusterStateHolder;
import com.ctrip.hermes.metaserver.cluster.Role;
import com.ctrip.hermes.metaserver.commons.ClientContext;
import com.ctrip.hermes.metaserver.commons.EndpointMaker;
import com.ctrip.hermes.metaserver.event.impl.BaseMetaChangedEventHandler;
import com.ctrip.hermes.metaserver.event.impl.LeaderInitEventHandler;
import com.ctrip.hermes.metaserver.event.impl.MetaServerListChangedEventHandler;
import com.ctrip.hermes.metaserver.meta.MetaHolder;
import com.ctrip.hermes.metaserver.meta.MetaServerAssignmentHolder;
import com.ctrip.hermes.metaservice.service.MetaService;
import com.ctrip.hermes.metaservice.zk.ZKPathUtils;
import com.ctrip.hermes.metaservice.zk.ZKSerializeUtils;

@RunWith(MockitoJUnitRunner.class)
public class LeaderEventEngineTest extends ZKSuppportTestCase {

	@Mock
	private MetaService m_metaService;

	@Mock
	private MetaHolder m_metaHolder;

	@Mock
	private BrokerAssignmentHolder m_brokerAssignmentHolder;

	@Mock
	private MetaServerAssignmentHolder m_metaServerAssignmentHolder;

	@Mock
	private EndpointMaker m_endpointMaker;

	private EventBus m_eventBus;

	private static AtomicInteger m_baseMetaChangeCount = new AtomicInteger(0);

	private static AtomicReference<List<Server>> m_metaServers = new AtomicReference<List<Server>>();

	@Override
	protected void initZkData() throws Exception {
		ensurePath("/brokers");
		ensurePath(ZKPathUtils.getMetaInfoZkPath());
		ensurePath(ZKPathUtils.getMetaServersZkPath());
		ensurePath(ZKPathUtils.getMetaServerAssignmentRootZkPath());
		ensurePath(ZKPathUtils.getBaseMetaVersionZkPath());

	}

	@Override
	protected void doSetUp() throws Exception {
		m_baseMetaChangeCount.set(0);
		defineComponent(EventHandler.class, "BaseMetaChangedEventHandler", TestBaseMetaChangedEventHandler.class);
		defineComponent(EventHandler.class, "MetaServerListChangedEventHandler",
		      TestMetaServerListChangedEventHandler.class);

		LeaderInitEventHandler leaderInitEventHandler = (LeaderInitEventHandler) lookup(EventHandler.class,
		      "LeaderInitEventHandler");
		leaderInitEventHandler.setMetaService(m_metaService);
		leaderInitEventHandler.setMetaServerAssignmentHolder(m_metaServerAssignmentHolder);
		leaderInitEventHandler.setMetaHolder(m_metaHolder);
		leaderInitEventHandler.setBrokerAssignmentHolder(m_brokerAssignmentHolder);

		when(m_metaService.refreshMeta()).thenReturn(TestHelper.loadLocalMeta(this));

		m_curator.setData().forPath(ZKPathUtils.getBaseMetaVersionZkPath(), ZKSerializeUtils.serialize(1L));

		leaderInitEventHandler.start();
		m_eventBus = lookup(EventBus.class);
	}

	private void setMetaServers(List<Pair<String, HostPort>> servers) throws Exception {
		deleteChildren(ZKPathUtils.getMetaServersZkPath(), false);
		for (Pair<String, HostPort> pair : servers) {
			String path = ZKPaths.makePath(ZKPathUtils.getMetaServersZkPath(), pair.getKey());
			ensurePath(path);
			m_curator.setData().forPath(path, ZKSerializeUtils.serialize(pair.getValue()));
		}
	}

	private void delMetaServer(String server) throws Exception {
		String path = ZKPaths.makePath(ZKPathUtils.getMetaServersZkPath(), server);
		m_curator.delete().forPath(path);
	}

	private void addMetaServer(String server, String host, int port) throws Exception {
		String path = ZKPaths.makePath(ZKPathUtils.getMetaServersZkPath(), server);
		ensurePath(path);
		m_curator.setData().forPath(path, ZKSerializeUtils.serialize(new HostPort(host, port)));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testStart() throws Exception {
		startEngine();
		verify(m_metaHolder, times(1)).update(anyMap());
		verify(m_metaServerAssignmentHolder, times(1))
		      .reassign(anyListOf(Server.class), anyMap(), anyListOf(Topic.class));
		verify(m_brokerAssignmentHolder, times(1)).reassign(anyMapOf(String.class, ClientContext.class),
		      anyListOf(Endpoint.class), anyListOf(Topic.class), anyMapOf(String.class, Idc.class));
	}

	@Test
	public void testBaseMetaChange() throws Exception {
		startEngine();

		m_curator.setData().forPath(ZKPathUtils.getBaseMetaVersionZkPath(), ZKSerializeUtils.serialize(100L));

		int retries = 50;
		int i = 0;
		while (m_baseMetaChangeCount.get() != 1 && i++ < retries) {
			TimeUnit.MILLISECONDS.sleep(100);
		}
		assertEquals(1, m_baseMetaChangeCount.get());

		m_curator.setData().forPath(ZKPathUtils.getBaseMetaVersionZkPath(), ZKSerializeUtils.serialize(200L));

		i = 0;
		while (m_baseMetaChangeCount.get() != 2 && i++ < retries) {
			TimeUnit.MILLISECONDS.sleep(100);
		}
		assertEquals(2, m_baseMetaChangeCount.get());
	}

	@Test
	public void testMetaServerListChange() throws Exception {
		startEngine();

		addMetaServer("ms2", "1.1.1.2", 2222);

		int retries = 50;
		int i = 0;
		while (m_metaServers.get() == null && i++ < retries) {
			TimeUnit.MILLISECONDS.sleep(100);
		}
		assertEquals(2, m_metaServers.get().size());
		assertTrue(m_metaServers.get().contains(new Server("ms1")));
		assertTrue(m_metaServers.get().contains(new Server("ms2")));

		m_metaServers.set(null);
		delMetaServer("ms2");
		i = 0;
		while (m_metaServers.get() == null && i++ < retries) {
			TimeUnit.MILLISECONDS.sleep(100);
		}
		assertEquals(1, m_metaServers.get().size());
		assertTrue(m_metaServers.get().contains(new Server("ms1")));
	}

	private void startEngine() throws Exception, InterruptedException {
		setMetaServers(Arrays.asList(//
		      new Pair<String, HostPort>("ms1", new HostPort("1.1.1.1", 1111))//
		      ));

		final CountDownLatch latch = new CountDownLatch(1);

		doAnswer(new Answer<Void>() {

			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				latch.countDown();
				return null;
			}
		}).when(m_metaServerAssignmentHolder).reassign(anyListOf(Server.class), anyMap(), anyListOf(Topic.class));

		m_eventBus.pubEvent(new Event(EventType.LEADER_INIT, 0, null));

		latch.await(5, TimeUnit.SECONDS);
	}

	private ClusterStateHolder createClusterStateHolder() {
		ClusterStateHolder holder = new ClusterStateHolder();
		holder.setRole(Role.LEADER);
		return holder;
	}

	public static class TestBaseMetaChangedEventHandler extends BaseMetaChangedEventHandler {

		@Override
		protected void processEvent(Event event) throws Exception {
			m_baseMetaChangeCount.incrementAndGet();
		}

	}

	public static class TestMetaServerListChangedEventHandler extends MetaServerListChangedEventHandler {
		@SuppressWarnings("unchecked")
		@Override
		protected void processEvent(Event event) throws Exception {
			m_metaServers.set((List<Server>) event.getData());
		}
	}
}
