package com.ctrip.hermes.broker.queue.storage.mysql.cache;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.broker.queue.storage.mysql.cache.MessageCache.MessageLoader;
import com.ctrip.hermes.broker.queue.storage.mysql.cache.MessageCache.ShrinkStrategy;
import com.ctrip.hermes.broker.queue.storage.mysql.cache.PageCache.PageLoader;
import com.ctrip.hermes.broker.queue.storage.mysql.cache.PageCache.ResizeListener;
import com.ctrip.hermes.broker.queue.storage.mysql.dal.IdAware;
import com.ctrip.hermes.broker.status.BrokerStatusMonitor;
import com.ctrip.hermes.core.utils.CollectionUtil;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Message;
import com.dianping.cat.message.Transaction;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class MessageCacheBuilder {

	private static final int DEFAULT_CONCURRENCY_LEVEL = 32;

	private static final int DEFAULT_MAXIMUM_MESSAGE_CAPACITY = 1024 * 1024;// if 1K bytes per message, maximumMessageCapacity will
	                                                                        // be 1GB

	private static final int DEFAULT_PAGE_CACHE_CORE_SIZE = 2;

	private static final int DEFAULT_PAGE_CACHE_MAXIMUM_SIZE = 8;

	private static final int DEFAULT_PAGE_LOADING_INTERVAL_MILLIS = 200;

	private static final int DEFAULT_PAGE_SIZE = 500; // if 1K bytes per message, each page will be 500K

	private int m_concurrencyLevel = DEFAULT_CONCURRENCY_LEVEL;

	private int m_defaultPageCacheCoreSize = DEFAULT_PAGE_CACHE_CORE_SIZE;

	private int m_defaultPageCacheMaximumSize = DEFAULT_PAGE_CACHE_MAXIMUM_SIZE;

	private int m_defaultPageSize = DEFAULT_PAGE_SIZE;

	private int m_maximumMessageCapacity = DEFAULT_MAXIMUM_MESSAGE_CAPACITY;

	private MessageLoader<?> m_messageLoader;

	private int m_pageLoadIntervalMillis = DEFAULT_PAGE_LOADING_INTERVAL_MILLIS;

	private Map<String, Pair<Integer, Integer>> m_topicPageCacheSize = new HashMap<>();

	private Map<String, Integer> m_topicPageSize = new HashMap<>();

	private String m_name = "default";

	private ShrinkStrategy<?> m_shrinkStrategy;

	private MessageCacheBuilder() {

	}

	public static MessageCacheBuilder newBuilder() {
		return new MessageCacheBuilder();
	}

	public <T extends IdAware> MessageCache<T> build() {
		checkNotNull(m_messageLoader);
		checkNotNull(m_shrinkStrategy);
		return new DefaultMessageCache<T>(this);
	}

	public MessageCacheBuilder concurrencyLevel(int concurrencyLevel) {
		m_concurrencyLevel = concurrencyLevel;
		return this;
	}

	public MessageCacheBuilder name(String name) {
		m_name = name;
		return this;
	}

	public MessageCacheBuilder defaultPageCacheCoreSize(int defaultPageCacheCoreSize) {
		m_defaultPageCacheCoreSize = defaultPageCacheCoreSize;
		return this;
	}

	public MessageCacheBuilder defaultPageCacheMaximumSize(int defaultPageCacheMaximumSize) {
		m_defaultPageCacheMaximumSize = defaultPageCacheMaximumSize;
		return this;
	}

	public MessageCacheBuilder defaultPageSize(int defaultPageSize) {
		m_defaultPageSize = defaultPageSize;
		return this;
	}

	public MessageCacheBuilder maximumMessageCapacity(int maximumMessageCapacity) {
		m_maximumMessageCapacity = maximumMessageCapacity;
		return this;
	}

	public MessageCacheBuilder messageLoader(MessageLoader<?> messageLoader) {
		m_messageLoader = messageLoader;
		return this;
	}

	public MessageCacheBuilder shrinkStrategy(ShrinkStrategy<?> shrinkStrategy) {
		m_shrinkStrategy = shrinkStrategy;
		return this;
	}

	public MessageCacheBuilder pageLoadIntervalMillis(int pageLoadIntervalMillis) {
		m_pageLoadIntervalMillis = pageLoadIntervalMillis;
		return this;
	}

	public MessageCacheBuilder topicPageCacheSize(String topic, int coreSize, int maximumSize) {
		m_topicPageCacheSize.put(topic, new Pair<>(coreSize, maximumSize));
		return this;
	}

	public MessageCacheBuilder topicPageSize(String topic, int pageSize) {
		m_topicPageSize.put(topic, pageSize);
		return this;
	}

	static class DefaultMessageCache<T extends IdAware> implements MessageCache<T> {

		private MessageCacheBuilder m_builder;

		private String m_name;

		private int m_defaultPageCacheCoreSize;

		private int m_defaultPageCacheMaximumSize;

		private int m_defaultPageSize;

		private MessageLoader<T> m_messageLoader;

		private ShrinkStrategy<T> m_shrinkStrategy;

		private ExecutorService m_pageLoaderThreadPool;

		private int m_pageLoadIntervalMillis;

		private Map<String, Pair<Integer, Integer>> m_topicPageCacheSize;

		private Map<String, Integer> m_topicPageSize;

		private LoadingCache<Pair<String, Integer>, PageCache<T>> m_tpPageCaches;

		private ExecutorService m_shrinkExecutor = Executors.newSingleThreadExecutor(HermesThreadFactory.create(
		      "MessageCacheShrinker", true));

		private AtomicBoolean m_shrinking = new AtomicBoolean(false);

		@SuppressWarnings("unchecked")
		DefaultMessageCache(MessageCacheBuilder builder) {
			m_builder = builder;
			m_name = builder.m_name;
			m_defaultPageSize = builder.m_defaultPageSize;
			m_topicPageSize = builder.m_topicPageSize;
			m_defaultPageCacheCoreSize = builder.m_defaultPageCacheCoreSize;
			m_defaultPageCacheMaximumSize = builder.m_defaultPageCacheMaximumSize;
			m_topicPageCacheSize = builder.m_topicPageCacheSize;
			m_pageLoadIntervalMillis = builder.m_pageLoadIntervalMillis;
			m_messageLoader = (MessageLoader<T>) builder.m_messageLoader;
			m_shrinkStrategy = (ShrinkStrategy<T>) builder.m_shrinkStrategy;

			m_pageLoaderThreadPool = Executors.newCachedThreadPool(HermesThreadFactory.create("MessageCacheLoader", true));

			m_tpPageCaches = buildCache();
		}

		private LoadingCache<Pair<String, Integer>, PageCache<T>> buildCache() {
			return CacheBuilder.newBuilder()//
			      .concurrencyLevel(m_builder.m_concurrencyLevel)//
			      .maximumWeight(m_builder.m_maximumMessageCapacity)//
			      .removalListener(new RemovalListener<Pair<String, Integer>, PageCache<T>>() {

				      @Override
				      public void onRemoval(RemovalNotification<Pair<String, Integer>, PageCache<T>> notification) {
					      if (m_shrinking.compareAndSet(false, true)) {
						      m_shrinkExecutor.submit(new Runnable() {

							      @Override
							      public void run() {
								      Transaction tx = Cat.newTransaction("Message.Broker.Cache.Shrink", m_name);
								      try {
									      int shrinkCount = m_shrinkStrategy.shrink(m_tpPageCaches.asMap().values());
									      tx.addData("count", shrinkCount);
									      tx.setStatus(Transaction.SUCCESS);
								      } catch (Exception e) {
									      Cat.logError(e);
									      tx.setStatus(e);
								      } finally {
									      m_shrinking.set(false);
									      tx.complete();
								      }
							      }
						      });
					      }
				      }
			      })//
			      .weigher(new Weigher<Pair<String, Integer>, PageCache<T>>() {

				      @Override
				      public int weigh(Pair<String, Integer> key, PageCache<T> pageCache) {
					      return pageCache.pageCount() * pageCache.pageSize();
				      }
			      })//
			      .build(new CacheLoader<Pair<String, Integer>, PageCache<T>>() {

				      @Override
				      public PageCache<T> load(Pair<String, Integer> tp) throws Exception {
					      return buildPageCache(tp);
				      }
			      });
		}

		private PageCache<T> buildPageCache(final Pair<String, Integer> tp) {
			String topic = tp.getKey();
			int partition = tp.getValue();
			int pageSize = getPageSize(topic);

			int coreSize = m_defaultPageCacheCoreSize;
			int maximumSize = m_defaultPageCacheMaximumSize;
			if (m_topicPageCacheSize.containsKey(topic)) {
				Pair<Integer, Integer> pair = m_topicPageCacheSize.get(topic);
				coreSize = pair.getKey();
				maximumSize = pair.getValue();
			}

			PageCache<T> pageCache = createPageCacheBuilder(topic, partition, pageSize, coreSize, maximumSize).build();
			BrokerStatusMonitor.INSTANCE.addCacheSizeGauge(tp.getKey(), tp.getValue(), m_name, pageCache);
			return pageCache;
		}

		private PageCacheBuilder createPageCacheBuilder(final String topic, final int partition, int pageSize,
		      int coreSize, int maximumSize) {
			return PageCacheBuilder.newBuilder()//
			      .pageSize(pageSize)//
			      .coreSize(coreSize)//
			      .maxSize(maximumSize)//
			      .pageLoadIntervalMillis(m_pageLoadIntervalMillis)//
			      .pageLoaderThreadPool(m_pageLoaderThreadPool)//
			      .pageLoader(new PageLoader<T>() {

				      @Override
				      public void loadPage(Page<T> page) {
					      if (!page.isComplete()) {
						      Transaction catTx = Cat.newTransaction("Message.Broker.Cache.Load", topic + "-" + m_name);
						      try {
							      long latestLoadedOffset = page.getLatestLoadedOffset();
							      int batchSize = (int) (page.getEndOffset() - latestLoadedOffset);
							      List<T> messages = m_messageLoader.load(topic, partition, latestLoadedOffset, batchSize);
							      if (CollectionUtil.isNotEmpty(messages)) {
								      List<Pair<Long, T>> idDataPairs = new ArrayList<>(messages.size());
								      for (T msg : messages) {
									      idDataPairs.add(new Pair<Long, T>(msg.getId(), msg));
								      }
								      page.addData(idDataPairs);
							      }
							      catTx.setStatus(Transaction.SUCCESS);
						      } catch (Exception e) {
							      Cat.logError("Exception occurred while loading page.", e);
							      catTx.setStatus(e);
						      } finally {
							      catTx.complete();
						      }
					      }
				      }
			      }).resizeListener(new ResizeListener<T>() {

				      @Override
				      public void onResize(PageCache<T> pageCache, int oldSize, int newSize) {
					      if (oldSize > newSize) {
						      Cat.logEvent("Message.Broker.PageCache.Shrink", topic, Message.SUCCESS,
						            String.format("oldSize=%s&newSize=%s", oldSize, newSize));
					      } else {
						      Cat.logEvent("Message.Broker.PageCache.Expand", topic, Message.SUCCESS,
						            String.format("oldSize=%s&newSize=%s", oldSize, newSize));
					      }

					      m_tpPageCaches.put(new Pair<>(topic, partition), pageCache);
				      }
			      });
		}

		private List<T> getFromPageCache(PageCache<T> pageCache, long startOffsetInclusive, int batchSize) {

			LinkedList<T> datas = new LinkedList<>();

			int loadedSize = 0;
			long pageNo = startOffsetInclusive / pageCache.pageSize();
			while (loadedSize < batchSize) {
				Page<T> page = pageCache.get(pageNo);

				if (!page.isComplete()) {
					readPageDataToResult(page, startOffsetInclusive, batchSize - loadedSize, datas);
					break;
				} else {
					loadedSize += readPageDataToResult(page, startOffsetInclusive, batchSize - loadedSize, datas);

					if (loadedSize < batchSize) {
						pageNo = page.getNextPageNo();
						startOffsetInclusive = pageNo * pageCache.pageSize();
						if (!page.endOffsetExists()) {
							Page<T> nextPage = waitUntilPageFilled(pageCache, pageNo, startOffsetInclusive, batchSize
							      - loadedSize, 5000);

							if (nextPage != null) {
								loadedSize += readPageDataToResult(nextPage, startOffsetInclusive, batchSize - loadedSize,
								      datas);
								startOffsetInclusive = datas.getLast().getId() + 1;
								pageNo = startOffsetInclusive / pageCache.pageSize();
							} else {
								break;
							}

						}
					}
				}
			}

			return datas;
		}

		private Page<T> waitUntilPageFilled(PageCache<T> pageCache, long pageNo, long startOffsetInclusive,
		      int batchSize, int timeoutMillis) {
			int maxRetries = timeoutMillis / m_pageLoadIntervalMillis;
			Page<T> page = pageCache.get(pageNo);
			int retries = 0;
			while (retries++ < maxRetries && !page.isFilled()) {
				try {
					TimeUnit.MILLISECONDS.sleep(m_pageLoadIntervalMillis);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					return null;
				}
			}

			return page.getDatas(startOffsetInclusive, batchSize).isEmpty() ? null : page;
		}

		private int readPageDataToResult(Page<T> page, long startOffsetInclusive, int batchSize, LinkedList<T> datas) {
			List<T> loadedData = page.getDatas(startOffsetInclusive, batchSize);
			datas.addAll(loadedData);
			return loadedData.size();
		}

		@Override
		public List<T> getOffsetAfter(String topic, int partition, long startOffsetExclusive, int batchSize) {
			Transaction catTx = Cat.newTransaction("Message.Broker.Cache.Get", topic + "-" + m_name);
			try {
				PageCache<T> pageCache = getPageCache(topic, partition);
				// find next offset in page cache
				List<T> rows = getFromPageCache(pageCache, startOffsetExclusive + 1, batchSize);
				BrokerStatusMonitor.INSTANCE.cacheCall(topic, partition, CollectionUtil.isNotEmpty(rows));
				return rows;
			} finally {
				catTx.setStatus(Transaction.SUCCESS);
				catTx.complete();
			}
		}

		private PageCache<T> getPageCache(String topic, int partition) {
			return m_tpPageCaches.getUnchecked(new Pair<String, Integer>(topic, partition));
		}

		private int getPageSize(String topic) {
			int pageSize = m_defaultPageSize;
			if (m_topicPageSize.containsKey(topic)) {
				pageSize = m_topicPageSize.get(topic);
			}
			return pageSize;
		}
	}
}
