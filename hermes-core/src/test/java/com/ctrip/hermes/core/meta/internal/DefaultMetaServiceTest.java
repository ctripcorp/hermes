package com.ctrip.hermes.core.meta.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.unidal.lookup.ComponentTestCase;
import org.xml.sax.SAXException;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.config.CoreConfig;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;
import com.ctrip.hermes.core.message.retry.FrequencySpecifiedRetryPolicy;
import com.ctrip.hermes.core.message.retry.RetryPolicy;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.meta.transform.DefaultSaxParser;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultMetaServiceTest extends ComponentTestCase {

	@Mock
	private TestMetaHolder m_metaHolder;

	private MetaService m_metaService;

	@Before
	@Override
	public void setUp() throws Exception {
		super.setUp();
		defineComponent(MetaService.class, TestMetaService.class).req(CoreConfig.class);
		((TestMetaService) lookup(MetaService.class)).setMetaHolder(m_metaHolder);

		when(m_metaHolder.getMeta()).thenReturn(loadLocalMeta());

		m_metaService = lookup(MetaService.class);
	}

	@Test
	public void testGetAckTimeoutSecondsByTopicAndConsumerGroup() throws Exception {
		assertEquals(10, m_metaService.getAckTimeoutSecondsByTopicAndConsumerGroup("test_broker", "group1"));
		assertEquals(6, m_metaService.getAckTimeoutSecondsByTopicAndConsumerGroup("test_broker", "group2"));
		assertEquals(5, m_metaService.getAckTimeoutSecondsByTopicAndConsumerGroup("test_broker", "group3"));

		try {
			m_metaService.getAckTimeoutSecondsByTopicAndConsumerGroup("topic_not_found", "group3");
			fail();
		} catch (RuntimeException e) {
			// do nothing
		} catch (Exception e) {
			fail();
		}

		try {
			m_metaService.getAckTimeoutSecondsByTopicAndConsumerGroup("test_broker", "group_not_found");
			fail();
		} catch (RuntimeException e) {
			// do nothing
		} catch (Exception e) {
			fail();
		}
	}

	@Test
	public void testListPartitionsByTopic() throws Exception {
		List<Partition> kafkaPartitions = m_metaService.listPartitionsByTopic("test_kafka");
		assertEquals(1, kafkaPartitions.size());
		assertEquals("kafka1", kafkaPartitions.get(0).getEndpoint());
		assertEquals(Integer.valueOf(0), kafkaPartitions.get(0).getId());

		List<Partition> brokerPartitions = m_metaService.listPartitionsByTopic("test_broker");
		assertEquals(2, brokerPartitions.size());
		assertEquals("br0", brokerPartitions.get(0).getEndpoint());
		assertEquals(Integer.valueOf(0), brokerPartitions.get(0).getId());
		assertEquals("br1", brokerPartitions.get(1).getEndpoint());
		assertEquals(Integer.valueOf(1), brokerPartitions.get(1).getId());
	}

	@Test(expected = RuntimeException.class)
	public void testListPartitionsByTopicTopicNotFound() throws Exception {
		m_metaService.listPartitionsByTopic("topicNotFound");
	}

	@Test
	public void testFindStorageByTopic() throws Exception {
		Storage brokerStorage = m_metaService.findStorageByTopic("test_broker");
		assertEquals(Storage.MYSQL, brokerStorage.getType());
		Storage kafkaStorage = m_metaService.findStorageByTopic("test_kafka");
		assertEquals(Storage.KAFKA, kafkaStorage.getType());
	}

	@Test(expected = RuntimeException.class)
	public void testFindStorageByTopicTopicNotFound() throws Exception {
		m_metaService.findStorageByTopic("topicNotFound");
	}

	@Test
	public void testFindPartitionByTopicAndPartition() throws Exception {
		Partition brokerPartition = m_metaService.findPartitionByTopicAndPartition("test_broker", 0);
		assertEquals("br0", brokerPartition.getEndpoint());
		assertEquals(Integer.valueOf(0), brokerPartition.getId());
		brokerPartition = m_metaService.findPartitionByTopicAndPartition("test_broker", 1);
		assertEquals("br1", brokerPartition.getEndpoint());
		assertEquals(Integer.valueOf(1), brokerPartition.getId());
		Partition kafkaPartition = m_metaService.findPartitionByTopicAndPartition("test_kafka", 0);
		assertEquals("kafka1", kafkaPartition.getEndpoint());
		assertEquals(Integer.valueOf(0), kafkaPartition.getId());
		assertNull(m_metaService.findPartitionByTopicAndPartition("test_kafka", 1));
	}

	@Test(expected = RuntimeException.class)
	public void testFindPartitionByTopicAndPartitionTopicNotFound() throws Exception {
		m_metaService.findPartitionByTopicAndPartition("topicNotFound", 0);
	}

	@Test
	public void testListTopicsByPattern() {
		DefaultMetaService ms = new DefaultMetaService();
		assertTrue(ms.isTopicMatch("a", "a"));
		assertTrue(ms.isTopicMatch("a.b", "a.b"));
		assertTrue(ms.isTopicMatch("a.b.c", "a.b.c"));

		assertTrue(ms.isTopicMatch("a.*", "a.bbb"));
		assertFalse(ms.isTopicMatch("a.*", "a"));
		assertFalse(ms.isTopicMatch("a.*", "a.b.c"));

		assertTrue(ms.isTopicMatch("a.#", "a.b"));
		assertFalse(ms.isTopicMatch("a.#", "a"));
		assertTrue(ms.isTopicMatch("a.#", "a.bbb"));
		assertTrue(ms.isTopicMatch("a.#", "a.bbb.ccc"));

		assertTrue(ms.isTopicMatch("a.*.c", "a.bbb.c"));
		assertFalse(ms.isTopicMatch("a.*.c", "a.bbb.xxx.c"));

		assertTrue(ms.isTopicMatch("a.#.c", "a.bbb.c"));
		assertTrue(ms.isTopicMatch("a.#.c", "a.bbb.xxx.c"));
		assertFalse(ms.isTopicMatch("a.#.c", "a.bbb.d"));
		assertFalse(ms.isTopicMatch("a.#.c", "a.bbb.xxx.d"));

		assertTrue(ms.isTopicMatch("a.*.c", "a.b--c.c"));
		assertTrue(ms.isTopicMatch("a.#.c", "a.b-c.d-d.c"));
		assertTrue(ms.isTopicMatch("a.#.c", "a.b-c..c"));
		assertFalse(ms.isTopicMatch("a.*.c", "a.b-c.dd.c"));
		assertFalse(ms.isTopicMatch("a.#.c", "a.b-c.d-d.d"));
		assertFalse(ms.isTopicMatch("a.#.c", "a.b-c..d"));
	}

	@Test
	public void testFindTopicByName() throws Exception {
		Topic topic = m_metaService.findTopicByName("test_broker");
		assertEquals("test_broker", topic.getName());
		topic = m_metaService.findTopicByName("topic_not_found");
		assertNull(topic);
	}

	@Test
	public void testTranslateToIntGroupId() throws Exception {
		assertEquals(1, m_metaService.translateToIntGroupId("test_broker", "group1"));
		assertEquals(2, m_metaService.translateToIntGroupId("test_broker", "group2"));
		assertEquals(3, m_metaService.translateToIntGroupId("test_broker", "group3"));

		try {
			m_metaService.translateToIntGroupId("topic_not_found", "group3");
			fail();
		} catch (RuntimeException e) {
			// do nothing
		} catch (Exception e) {
			fail();
		}

		try {
			m_metaService.translateToIntGroupId("test_broker", "group_not_found");
			fail();
		} catch (RuntimeException e) {
			// do nothing
		} catch (Exception e) {
			fail();
		}
	}

	@Test
	public void testListAllMysqlDataSources() throws Exception {
		List<Datasource> datasources = m_metaService.listAllMysqlDataSources();
		assertEquals(2, datasources.size());
		assertEquals("ds0", datasources.get(0).getId());
		assertEquals("ds1", datasources.get(1).getId());
	}

	@Test
	public void testContainsConsumerGroup() throws Exception {
		assertTrue(m_metaService.containsConsumerGroup("test_broker", "group1"));
		assertFalse(m_metaService.containsConsumerGroup("test_broker", "group5"));

		try {
			m_metaService.containsConsumerGroup("topic_not_found", "group3");
			fail();
		} catch (RuntimeException e) {
			// do nothing
		} catch (Exception e) {
			fail();
		}
	}

	@Test
	public void testContainsEndpoint() throws Exception {
		assertTrue(m_metaService.containsEndpoint(new Endpoint("br0")));
		assertFalse(m_metaService.containsEndpoint(new Endpoint("notFound")));
	}

	@Test
	public void testFindRetryPolicyByTopicAndGroup() throws Exception {
		RetryPolicy policy = m_metaService.findRetryPolicyByTopicAndGroup("test_broker", "group1");
		assertTrue(policy instanceof FrequencySpecifiedRetryPolicy);
		assertEquals(4, ((FrequencySpecifiedRetryPolicy) policy).getRetryTimes());

		policy = m_metaService.findRetryPolicyByTopicAndGroup("test_broker", "group2");
		assertTrue(policy instanceof FrequencySpecifiedRetryPolicy);
		assertEquals(2, ((FrequencySpecifiedRetryPolicy) policy).getRetryTimes());

		policy = m_metaService.findRetryPolicyByTopicAndGroup("test_broker", "group3");
		assertTrue(policy instanceof FrequencySpecifiedRetryPolicy);
		assertEquals(3, ((FrequencySpecifiedRetryPolicy) policy).getRetryTimes());

		try {
			m_metaService.findRetryPolicyByTopicAndGroup("topic_not_found", "group3");
			fail();
		} catch (RuntimeException e) {
			// do nothing
		} catch (Exception e) {
			fail();
		}

		try {
			m_metaService.findRetryPolicyByTopicAndGroup("test_broker", "group_not_found");
			fail();
		} catch (RuntimeException e) {
			// do nothing
		} catch (Exception e) {
			fail();
		}
	}

	@Test
	public void testFindEndpointByTopicAndPartition() throws Exception {
		Endpoint endpoint = m_metaService.findEndpointByTopicAndPartition("test_broker", 0).getKey();
		assertEquals("br0", endpoint.getId());
		endpoint = m_metaService.findEndpointByTopicAndPartition("test_broker", 1).getKey();
		assertEquals("br1", endpoint.getId());

		try {
			m_metaService.findEndpointByTopicAndPartition("topic_not_found", 0).getKey();
			fail();
		} catch (RuntimeException e) {
			// do nothing
		} catch (Exception e) {
			fail();
		}

		try {
			m_metaService.findEndpointByTopicAndPartition("test_broker", 100);
			fail();
		} catch (RuntimeException e) {
			// do nothing
		} catch (Exception e) {
			fail();
		}
	}


	private Meta loadLocalMeta() throws Exception {
		String fileName = getClass().getSimpleName() + "-meta.xml";
		InputStream in = getClass().getResourceAsStream(fileName);

		if (in == null) {
			throw new RuntimeException(String.format("File %s not found in classpath.", fileName));
		} else {
			try {
				return DefaultSaxParser.parse(in);
			} catch (SAXException e) {
				throw new RuntimeException(String.format("Error parse meta file %s", fileName), e);
			} catch (IOException e) {
				throw new RuntimeException(String.format("Error parse meta file %s", fileName), e);
			}
		}
	}

	public static class TestMetaService extends DefaultMetaService {

		private TestMetaHolder m_metaHolder;

		private MetaProxy m_metaProxy = new TestMetaProxy();

		public void setMetaHolder(TestMetaHolder metaHolder) {
			m_metaHolder = metaHolder;
		}

		public void setMetaProxy(MetaProxy metaProxy) {
			m_metaProxy = metaProxy;
		}

		@Override
		protected Meta getMeta() {
			return m_metaHolder.getMeta();
		}

		@Override
		protected MetaProxy getMetaProxy() {
			return m_metaProxy;
		}

		@Override
		public void initialize() throws InitializationException {
			// do nothing
		}

	}

	public static interface TestMetaHolder {
		public Meta getMeta();
	}

	public static class TestMetaProxy extends LocalMetaProxy {

		private AtomicLong m_leaseId = new AtomicLong(0);

		@Override
		public LeaseAcquireResponse tryAcquireConsumerLease(Tpg tpg, String sessionId) {
			long expireTime = System.currentTimeMillis() + 10 * 1000L;
			long leaseId = m_leaseId.incrementAndGet();
			return new LeaseAcquireResponse(true, new Lease(leaseId, expireTime), expireTime);
		}

		@Override
		public LeaseAcquireResponse tryRenewConsumerLease(Tpg tpg, Lease lease, String sessionId) {
			return new LeaseAcquireResponse(false, null, System.currentTimeMillis() + 10 * 1000L);
		}

		@Override
		public LeaseAcquireResponse tryRenewBrokerLease(String topic, int partition, Lease lease, String sessionId,
		      int brokerPort) {
			long expireTime = System.currentTimeMillis() + 10 * 1000L;
			long leaseId = m_leaseId.incrementAndGet();
			return new LeaseAcquireResponse(true, new Lease(leaseId, expireTime), expireTime);
		}

		@Override
		public LeaseAcquireResponse tryAcquireBrokerLease(String topic, int partition, String sessionId, int brokerPort) {
			long expireTime = System.currentTimeMillis() + 10 * 1000L;
			long leaseId = m_leaseId.incrementAndGet();
			return new LeaseAcquireResponse(true, new Lease(leaseId, expireTime), expireTime);
		}

	}

}
