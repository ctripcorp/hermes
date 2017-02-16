package com.ctrip.hermes.broker.queue.storage.mysql.cache;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ctrip.hermes.broker.queue.storage.mysql.cache.MessageCache.DefaultShrinkStrategy;
import com.ctrip.hermes.broker.queue.storage.mysql.cache.MessageCache.MessageLoader;
import com.ctrip.hermes.broker.queue.storage.mysql.dal.IdAware;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class DefaultMessageCacheTest {

	@Test
	public void testWithTopicSpecifiedPageConfigAndShrink() throws Exception {
		MessageCache<TestValue> cache = MessageCacheBuilder.newBuilder()//
		      .concurrencyLevel(1)//
		      .defaultPageSize(10)//
		      .defaultPageCacheCoreSize(2)//
		      .defaultPageCacheMaximumSize(10)//
		      .maximumMessageCapacity(10 * 10)//
		      .name("cache")//
		      .shrinkStrategy(new DefaultShrinkStrategy<TestValue>(0))//
		      .topicPageCacheSize("t1", 4, 16)//
		      .topicPageSize("t1", 5)//
		      .messageLoader(new TestMessageLoader()).build();

		assertGetTopic(cache, "t2", 2, 32, 24);
		assertGetTopic(cache, "t1", 0, 2, 45);
		assertGetTopic(cache, "t2", 0, 9, 39);
		assertGetTopic(cache, "t1", 1, -1, 74);
	}

	@Test
	public void testWithoutPageCacheResizeAndNoShrink() throws Exception {

		MessageCache<TestValue> cache = MessageCacheBuilder.newBuilder()//
		      .concurrencyLevel(1)//
		      .defaultPageSize(10)//
		      .defaultPageCacheCoreSize(10)//
		      .defaultPageCacheMaximumSize(10)//
		      .maximumMessageCapacity(10 * 10)//
		      .shrinkStrategy(new DefaultShrinkStrategy<TestValue>(5000))//
		      .messageLoader(new TestMessageLoader()).build();

		assertGetTopic(cache, "t1", 0, 2, 45);
		assertGetTopic(cache, "t1", 1, -1, 74);
		assertGetTopic(cache, "t2", 2, 32, 24);
		assertGetTopic(cache, "t2", 0, 9, 39);
	}

	@Test
	public void testWithPageCacheResizeAndNoShrink2() throws Exception {

		MessageCache<TestValue> cache = MessageCacheBuilder.newBuilder()//
		      .concurrencyLevel(1)//
		      .defaultPageSize(10)//
		      .defaultPageCacheCoreSize(2)//
		      .defaultPageCacheMaximumSize(10)//
		      .maximumMessageCapacity(10 * 10)//
		      .name("cache")//
		      .shrinkStrategy(new DefaultShrinkStrategy<TestValue>(5000))//
		      .messageLoader(new TestMessageLoader()).build();

		assertGetTopic(cache, "t1", 0, 2, 45);
		assertGetTopic(cache, "t1", 1, -1, 74);
		assertGetTopic(cache, "t2", 2, 32, 24);
		assertGetTopic(cache, "t2", 0, 9, 39);
	}

	@Test
	public void testWithLargeOffsetGap() throws Exception {

		final int gap = 999999;

		MessageCache<TestValue> cache = MessageCacheBuilder.newBuilder()//
		      .concurrencyLevel(1)//
		      .defaultPageSize(10)//
		      .defaultPageCacheCoreSize(6)//
		      .defaultPageCacheMaximumSize(6)//
		      .maximumMessageCapacity(60)//
		      .name("cache")//
		      .shrinkStrategy(new DefaultShrinkStrategy<TestValue>(5000))//
		      .messageLoader(new MessageLoader<TestValue>() {

			      @Override
			      public List<TestValue> load(String topic, int partition, long startOffsetExclusive, int batchSize) {
				      if (startOffsetExclusive < gap) {
					      return Arrays.asList(new TestValue(gap + 1, topic, partition));
				      } else {
					      List<TestValue> res = new ArrayList<>(batchSize);
					      for (int i = 0; i < batchSize; i++) {
						      res.add(new TestValue(startOffsetExclusive + i + 1, topic, partition));
					      }

					      return res;
				      }
			      }
		      }).build();

		List<TestValue> list = null;
		int retries = 0;
		int batchSize = 45;
		while ((list == null || list.size() != batchSize) && retries++ < 25) {
			list = cache.getOffsetAfter("t1", 1, 1, batchSize);
			TimeUnit.MILLISECONDS.sleep(200);
		}
		assertEquals(batchSize, list.size());
		for (int i = 0; i < batchSize; i++) {
			TestValue value = list.get(i);
			assertEquals(i + gap + 1, value.getId());
			assertEquals("t1", value.getTopic());
			assertEquals(1, value.getPartition());
		}

	}

	@Test
	public void testTwoOffsetGap() throws Exception {

		final int pageSize = 10;

		MessageCache<TestValue> cache = MessageCacheBuilder.newBuilder()//
		      .concurrencyLevel(1)//
		      .defaultPageSize(pageSize)//
		      .defaultPageCacheCoreSize(6)//
		      .defaultPageCacheMaximumSize(6)//
		      .maximumMessageCapacity(60)//
		      .name("cache")//
		      .shrinkStrategy(new DefaultShrinkStrategy<TestValue>(5000))//
		      .messageLoader(new MessageLoader<TestValue>() {

			      @Override
			      public List<TestValue> load(String topic, int partition, long startOffsetExclusive, int batchSize) {
				      long startOffsetInclusive = startOffsetExclusive + 1;
				      long pageNo = startOffsetInclusive / pageSize;

				      if (pageNo == 100) {
					      return Arrays.asList(new TestValue(1000, topic, partition), new TestValue(1003, topic, partition),
					            new TestValue(1030, topic, partition));
				      } else if (pageNo == 103) {
					      return Arrays.asList(new TestValue(1030, topic, partition), new TestValue(1039, topic, partition),
					            new TestValue(1053, topic, partition));
				      } else if (pageNo == 105) {
					      return Arrays.asList(new TestValue(1053, topic, partition), new TestValue(1055, topic, partition));
				      } else if (pageNo == 104) {
					      return Arrays.asList(new TestValue(1053, topic, partition));
				      } else if (pageNo < 100) {
					      return Arrays.asList(new TestValue(1000, topic, partition));
				      } else {
					      return new ArrayList<TestValue>();
				      }

			      }
		      }).build();

		List<TestValue> list = null;
		int retries = 0;
		int batchSize = 6;
		while ((list == null || list.size() != batchSize) && retries++ < 25) {
			list = cache.getOffsetAfter("t1", 1, 1, batchSize);
			TimeUnit.MILLISECONDS.sleep(200);
		}
		assertEquals(batchSize, list.size());
		assertEquals(1000, list.get(0).getId());
		assertEquals(1003, list.get(1).getId());
		assertEquals(1030, list.get(2).getId());
		assertEquals(1039, list.get(3).getId());
		assertEquals(1053, list.get(4).getId());
		assertEquals(1055, list.get(5).getId());

	}

	private void assertGetTopic(MessageCache<TestValue> cache, String topic, int partition, long startOffsetExclusive,
	      int batchSize) throws InterruptedException {
		List<TestValue> list = null;
		int retries = 0;
		while ((list == null || list.size() != batchSize) && retries++ < 1000) {
			list = cache.getOffsetAfter(topic, partition, startOffsetExclusive, batchSize);
			TimeUnit.MILLISECONDS.sleep(5);
		}
		assertEquals(batchSize, list.size());
		for (int i = 0; i < batchSize; i++) {
			TestValue value = list.get(i);
			assertEquals(i + startOffsetExclusive + 1, value.getId());
			assertEquals(topic, value.getTopic());
			assertEquals(partition, value.getPartition());
		}
	}

	private static class TestMessageLoader implements MessageLoader<TestValue> {

		@Override
		public List<TestValue> load(String topic, int partition, long startOffsetExclusive, int batchSize) {
			List<TestValue> res = new ArrayList<>(batchSize);
			for (int i = 0; i < batchSize; i++) {
				res.add(new TestValue(startOffsetExclusive + i + 1, topic, partition));
			}

			return res;
		}

	}

	private static class TestValue implements IdAware {

		private long m_id;

		private String m_topic;

		private int m_partition;

		public TestValue(long id, String topic, int partition) {
			m_id = id;
			m_topic = topic;
			m_partition = partition;
		}

		public long getId() {
			return m_id;
		}

		public String getTopic() {
			return m_topic;
		}

		public int getPartition() {
			return m_partition;
		}

		@Override
		public String toString() {
			return String.valueOf(m_id);
		}

	}
}
