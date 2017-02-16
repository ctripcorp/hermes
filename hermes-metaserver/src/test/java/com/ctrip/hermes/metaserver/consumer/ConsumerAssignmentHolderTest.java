package com.ctrip.hermes.metaserver.consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.metaserver.TestHelper;
import com.ctrip.hermes.metaserver.commons.Assignment;
import com.ctrip.hermes.metaserver.commons.ClientContext;
import com.ctrip.hermes.metaserver.config.MetaServerConfig;
import com.ctrip.hermes.metaserver.meta.MetaHolder;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class ConsumerAssignmentHolderTest {
	@Mock
	private MetaServerConfig m_config;

	@Mock
	private MetaHolder m_metaHolder;

	@Mock
	private ActiveConsumerListHolder m_activeConsumerListHolder;

	private ConsumerAssignmentHolder m_holder;

	@Before
	public void before() throws Exception {
		m_holder = new ConsumerAssignmentHolder();
		m_holder.setActiveConsumerListHolder(m_activeConsumerListHolder);
		m_holder.setConfig(m_config);
		m_holder.setMetaHolder(m_metaHolder);
		m_holder.setPartitionAssigningStrategy(new DefaultConsumerPartitionAssigningStrategy());

		when(m_config.getConsumerHeartbeatTimeoutMillis()).thenReturn(1000L);
		when(m_metaHolder.getMeta()).thenReturn(TestHelper.loadLocalMeta(this));
	}

	@Test
	public void testWithoutData() throws Exception {
		m_holder.rebalance();
		assertTrue(m_holder.getAssignments().isEmpty());
	}

	@Test
	public void test() throws Exception {
		Map<Pair<String, String>, Map<String, ClientContext>> changes1 = new HashMap<>();
		Pair<String, String> t1g1 = new Pair<String, String>("t1", "g1");
		Map<String, ClientContext> t1g1Consumers = new LinkedHashMap<>();
		t1g1Consumers.put("c1", new ClientContext("c1", "1.1.1.1", 1111, null, null, -1L));
		t1g1Consumers.put("c2", new ClientContext("c2", "2.2.2.2", 2222, null, null, -2L));
		changes1.put(t1g1, t1g1Consumers);

		// consuemr group not exist
		Pair<String, String> t1g4 = new Pair<String, String>("t1", "g4");
		Map<String, ClientContext> t1g4Consumers = new LinkedHashMap<>();
		t1g4Consumers.put("c1", new ClientContext("c1", "1.1.1.1", 1111, null, null, -1L));
		changes1.put(t1g4, t1g4Consumers);

		Pair<String, String> t1g2 = new Pair<String, String>("t1", "g2");
		Map<String, ClientContext> t1g2Consumers = new LinkedHashMap<>();
		t1g2Consumers.put("c3", new ClientContext("c3", "3.3.3.3", 3333, null, null, -3L));
		t1g2Consumers.put("c4", new ClientContext("c4", "4.4.4.4", 4444, null, null, -4L));
		changes1.put(t1g2, t1g2Consumers);

		Pair<String, String> t2g1 = new Pair<String, String>("t2", "g1");
		Map<String, ClientContext> t2g1Consumers = new LinkedHashMap<>();
		t2g1Consumers.put("c1", new ClientContext("c1", "1.1.1.1", 1111, null, null, -1L));
		t2g1Consumers.put("c4", new ClientContext("c4", "4.4.4.4", 4444, null, null, -4L));
		changes1.put(t2g1, t2g1Consumers);

		Pair<String, String> t2g2 = new Pair<String, String>("t2", "g2");
		Map<String, ClientContext> t2g2Consumers = new LinkedHashMap<>();
		t2g2Consumers.put("c2", new ClientContext("c2", "2.2.2.2", 2222, null, null, -2L));
		t2g2Consumers.put("c3", new ClientContext("c3", "3.3.3.3", 3333, null, null, -3L));
		changes1.put(t2g2, t2g2Consumers);

		// topic not exist
		Pair<String, String> t3g1 = new Pair<String, String>("t3", "g1");
		Map<String, ClientContext> t3g1Consumers = new LinkedHashMap<>();
		t3g1Consumers.put("c2", new ClientContext("c2", "2.2.2.2", 2222, null, null, -2L));
		changes1.put(t3g1, t3g1Consumers);

		when(m_activeConsumerListHolder.scanChanges(anyLong(), any(TimeUnit.class))).thenReturn(changes1);
		// rebalance from empty state
		m_holder.rebalance();

		// t1g1
		assertAssignment(t1g1, Arrays.asList(
		      //
		      new Pair<Integer, List<ClientContext>>(0, Arrays.asList(new ClientContext("c1", "1.1.1.1", 1111, null,
		            null, -1L))),//
		      new Pair<Integer, List<ClientContext>>(1, Arrays.asList(new ClientContext("c2", "2.2.2.2", 2222, null,
		            null, -2L)))//
		      )//
		);
		// t1g2
		assertAssignment(t1g2, Arrays.asList(
		      //
		      new Pair<Integer, List<ClientContext>>(0, Arrays.asList(new ClientContext("c3", "3.3.3.3", 3333, null,
		            null, -3L), new ClientContext("c4", "4.4.4.4", 4444, null, null, -4L))),//
		      new Pair<Integer, List<ClientContext>>(1, Arrays.asList(new ClientContext("c3", "3.3.3.3", 3333, null,
		            null, -3L), new ClientContext("c4", "4.4.4.4", 4444, null, null, -4L)))//
		      )//
		);
		// t2g1
		assertAssignment(t2g1, Arrays.asList(
		      //
		      new Pair<Integer, List<ClientContext>>(0, Arrays.asList(new ClientContext("c1", "1.1.1.1", 1111, null,
		            null, -1L), new ClientContext("c4", "4.4.4.4", 4444, null, null, -4L))),//
		      new Pair<Integer, List<ClientContext>>(1, Arrays.asList(new ClientContext("c1", "1.1.1.1", 1111, null,
		            null, -1L), new ClientContext("c4", "4.4.4.4", 4444, null, null, -4L)))//
		      )//
		);
		// t2g2
		assertAssignment(t2g2, Arrays.asList(
		      //
		      new Pair<Integer, List<ClientContext>>(0, Arrays.asList(new ClientContext("c2", "2.2.2.2", 2222, null,
		            null, -2L))),//
		      new Pair<Integer, List<ClientContext>>(1, Arrays.asList(new ClientContext("c3", "3.3.3.3", 3333, null,
		            null, -3L)))//
		      )//
		);

		Map<Pair<String, String>, Map<String, ClientContext>> changes2 = new HashMap<>(changes1);
		changes2.get(t1g1).remove("c1");
		changes2.get(t1g2).remove("c3");
		changes2.get(t2g2).remove("c2");
		changes2.get(t2g2).put("c4", new ClientContext("c4", "4.4.4.4", 4444, null, null, -4L));
		reset(m_activeConsumerListHolder);
		when(m_activeConsumerListHolder.scanChanges(anyLong(), any(TimeUnit.class))).thenReturn(changes2);
		// rebanlance again
		m_holder.rebalance();

		// t1g1
		assertAssignment(t1g1, Arrays.asList(
		      //
		      new Pair<Integer, List<ClientContext>>(0, Arrays.asList(new ClientContext("c2", "2.2.2.2", 2222, null,
		            null, -2L))),//
		      new Pair<Integer, List<ClientContext>>(1, Arrays.asList(new ClientContext("c2", "2.2.2.2", 2222, null,
		            null, -2L)))//
		      )//
		);
		// t1g2
		assertAssignment(t1g2, Arrays.asList(
		      //
		      new Pair<Integer, List<ClientContext>>(0, Arrays.asList(new ClientContext("c4", "4.4.4.4", 4444, null,
		            null, -4L))),//
		      new Pair<Integer, List<ClientContext>>(1, Arrays.asList(new ClientContext("c4", "4.4.4.4", 4444, null,
		            null, -4L)))//
		      )//
		);
		// t2g1
		assertAssignment(t2g1, Arrays.asList(
		      //
		      new Pair<Integer, List<ClientContext>>(0, Arrays.asList(new ClientContext("c1", "1.1.1.1", 1111, null,
		            null, -1L), new ClientContext("c4", "4.4.4.4", 4444, null, null, -4L))),//
		      new Pair<Integer, List<ClientContext>>(1, Arrays.asList(new ClientContext("c1", "1.1.1.1", 1111, null,
		            null, -1L), new ClientContext("c4", "4.4.4.4", 4444, null, null, -4L)))//
		      )//
		);
		// t2g2
		assertAssignment(t2g2, Arrays.asList(
		      //
		      new Pair<Integer, List<ClientContext>>(0, Arrays.asList(new ClientContext("c3", "3.3.3.3", 3333, null,
		            null, -3L))),//
		      new Pair<Integer, List<ClientContext>>(1, Arrays.asList(new ClientContext("c4", "4.4.4.4", 4444, null,
		            null, -4L)))//
		      )//
		);
	}

	private void assertAssignment(Pair<String, String> tg,
	      List<Pair<Integer, List<ClientContext>>> expectedPartition2ConsumersList) throws Exception {
		Assignment<Integer> assignment = m_holder.getAssignment(tg);
		assertNotNull(assignment);

		for (Pair<Integer, List<ClientContext>> expectedPartition2Consumers : expectedPartition2ConsumersList) {
			int partition = expectedPartition2Consumers.getKey();
			List<ClientContext> expectedConsumers = expectedPartition2Consumers.getValue();
			Map<String, ClientContext> actualConsumerInfos = assignment.getAssignment(partition);
			assertEquals(expectedConsumers.size(), actualConsumerInfos.size());

			for (ClientContext expectedConsumer : expectedConsumers) {
				ClientContext actualConsumer = actualConsumerInfos.get(expectedConsumer.getName());
				assertEquals(expectedConsumer.getName(), actualConsumer.getName());
				assertEquals(expectedConsumer.getIp(), actualConsumer.getIp());
				assertEquals(expectedConsumer.getPort(), actualConsumer.getPort());
				assertEquals(expectedConsumer.getLastHeartbeatTime(), actualConsumer.getLastHeartbeatTime());
			}
		}
	}
}
