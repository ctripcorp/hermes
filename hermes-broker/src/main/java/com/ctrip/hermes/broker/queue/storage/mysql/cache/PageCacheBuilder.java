package com.ctrip.hermes.broker.queue.storage.mysql.cache;

import static com.google.common.base.Preconditions.checkNotNull;

import java.lang.reflect.Method;
import java.util.Queue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.helper.Reflects;

import com.ctrip.hermes.broker.queue.storage.mysql.cache.PageCache.PageLoader;
import com.ctrip.hermes.broker.queue.storage.mysql.cache.PageCache.ResizeListener;
import com.dianping.cat.Cat;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class PageCacheBuilder {

	private static final int DEFAULT_CORESIZE = 2;

	private static final int DEFAULT_MAXSIZE = 16;

	private static final long DEFAULT_PAGE_LOADING_INTERVAL_MILLIS = 200;

	private static final int DEFAULT_PAGESIZE = 500;

	private static final Logger log = LoggerFactory.getLogger(PageCacheBuilder.class);

	private int m_coreSize = DEFAULT_CORESIZE;

	private int m_maxSize = DEFAULT_MAXSIZE;

	private PageLoader<?> m_pageLoader;

	private ExecutorService m_pageLoaderThreadPool;

	private long m_pageLoadIntervalMillis = DEFAULT_PAGE_LOADING_INTERVAL_MILLIS;

	private int m_pageSize = DEFAULT_PAGESIZE;

	private ResizeListener<?> m_resizeListener;

	private PageCacheBuilder() {

	}

	public static PageCacheBuilder newBuilder() {
		return new PageCacheBuilder();
	}

	public <T> PageCache<T> build() {
		checkNotNull(m_pageLoader);
		checkNotNull(m_pageLoaderThreadPool);
		return new DefaultPageCache<T>(this);
	}

	public PageCacheBuilder coreSize(int coreSize) {
		m_coreSize = coreSize;
		return this;
	}

	public PageCacheBuilder maxSize(int maxSize) {
		m_maxSize = maxSize;
		return this;
	}

	public PageCacheBuilder pageLoader(PageLoader<?> pageLoader) {
		m_pageLoader = pageLoader;
		return this;
	}

	public PageCacheBuilder pageLoaderThreadPool(ExecutorService pageLoaderThreadPool) {
		m_pageLoaderThreadPool = pageLoaderThreadPool;
		return this;
	}

	public PageCacheBuilder pageLoadIntervalMillis(int pageLoadIntervalMillis) {
		m_pageLoadIntervalMillis = pageLoadIntervalMillis;
		return this;
	}

	public PageCacheBuilder pageSize(int pageSize) {
		m_pageSize = pageSize;
		return this;
	}

	public PageCacheBuilder resizeListener(ResizeListener<?> resizeListener) {
		m_resizeListener = resizeListener;
		return this;
	}

	static class DefaultPageCache<T> implements PageCache<T> {

		private AtomicReference<LoadingCache<Long, Page<T>>> m_cache = new AtomicReference<>();

		private AtomicReference<Cache<Long, Boolean>> m_recentlyExpiredPagesCache = new AtomicReference<>();

		private int m_coreSize;

		private int m_maxSize;

		private AtomicInteger m_size = new AtomicInteger();

		private AtomicLong m_lastResizeTimeMillis = new AtomicLong();

		private int m_pageSize;

		private long m_pageLoadIntervalMillis;

		private ExecutorService m_pageLoaderThreadPool;

		private PageLoader<T> m_pageLoader;

		private ResizeListener<T> m_resizeListener;

		@SuppressWarnings("unchecked")
		private DefaultPageCache(PageCacheBuilder builder) {
			m_pageSize = builder.m_pageSize;
			m_coreSize = builder.m_coreSize;
			m_pageLoadIntervalMillis = builder.m_pageLoadIntervalMillis;
			m_resizeListener = (ResizeListener<T>) builder.m_resizeListener;
			m_pageLoader = (PageLoader<T>) builder.m_pageLoader;
			m_pageLoaderThreadPool = builder.m_pageLoaderThreadPool;
			if (builder.m_coreSize > builder.m_maxSize) {
				m_maxSize = builder.m_coreSize;
				log.warn("Bad maxSize(coreSize:{}, maxSize:{}), will use {} as maxSize.", builder.m_coreSize,
				      builder.m_maxSize, builder.m_coreSize);
			} else {
				m_maxSize = builder.m_maxSize;
			}

			m_size.set(m_coreSize);
			m_lastResizeTimeMillis.set(System.currentTimeMillis());
			m_recentlyExpiredPagesCache.set(buildRecentlyExpiredPagesCache());
			m_cache.set(buildCache(m_coreSize));
		}

		private Cache<Long, Boolean> buildRecentlyExpiredPagesCache() {
			int size = m_maxSize - m_size.get() + 1;
			return CacheBuilder.newBuilder().initialCapacity(size).maximumSize(size).build();
		}

		private LoadingCache<Long, Page<T>> buildCache(int size) {
			return CacheBuilder.newBuilder().concurrencyLevel(1).initialCapacity(size).maximumSize(size)
			      .removalListener(new RemovalListener<Long, Page<T>>() {

				      @Override
				      public void onRemoval(RemovalNotification<Long, Page<T>> notification) {
					      m_recentlyExpiredPagesCache.get().put(notification.getKey(), true);
				      }
			      }).build(new CacheLoader<Long, Page<T>>() {

				      @Override
				      public Page<T> load(Long pageNo) throws Exception {
					      return new Page<>(pageNo, m_pageSize, m_pageLoadIntervalMillis);
				      }

			      });
		}

		// for test
		boolean contains(long pageNo) {
			return m_cache.get().asMap().containsKey(pageNo);
		}

		@SuppressWarnings("rawtypes")
		private void copyCacheDatas(Cache<Long, Page<T>> from, Cache<Long, Page<T>> to) {
			ConcurrentMap<Long, Page<T>> asMap = from.asMap();

			Object[] segments = (Object[]) Reflects.forField().getDeclaredFieldValue(asMap, "segments");
			Object segment = segments[0];
			Queue accessQueue = (Queue) Reflects.forField().getDeclaredFieldValue(segment, "accessQueue");

			ReentrantLock lock = ((ReentrantLock) segment);
			lock.lock();
			try {
				for (Object item : accessQueue) {
					try {
						Method m1 = Reflects.forMethod().getMethod(item.getClass(), "getKey");
						m1.setAccessible(true);
						Long key = (Long) m1.invoke(item);
						Method m2 = Reflects.forMethod().getMethod(item.getClass(), "getValueReference");
						m2.setAccessible(true);
						Object valueRef = m2.invoke(item);
						Page<T> value = Reflects.forMethod().invokeDeclaredMethod(valueRef, "get");

						to.put(key, value);
					} catch (Exception e) {
						log.error("Exception occurred while copying data from old cache to new cache.", e);
					}
				}
			} finally {
				lock.unlock();
			}
		}

		@Override
		public Page<T> get(long pageNo) {
			Page<T> page = m_cache.get().getUnchecked(pageNo);

			if (page.isFresh()) {// cache miss
				boolean hitRecentlyExpiredPages = m_recentlyExpiredPagesCache.get().getIfPresent(pageNo) != null;
				if (hitRecentlyExpiredPages) {
					if (m_size.get() < m_maxSize) {
						// expand
						resize(Math.min(m_size.get() << 1, m_maxSize));
					}
				}
			}

			if (!page.isComplete()) {
				loadPage(page);
			}

			return page;
		}

		private void loadPage(final Page<T> page) {
			if (page != null && page.startLoading()) {
				m_pageLoaderThreadPool.submit(new Runnable() {

					@Override
					public void run() {
						try {
							m_pageLoader.loadPage(page);
						} catch (Exception e) {
							Cat.logError("Exception occurred while loading page", e);
							log.error("Exception occurred while loading page", e);
						} finally {
							page.endLoading();
						}
					}
				});
			}
		}

		private void notifyResize(int oldSize, int newSize) {
			if (m_resizeListener != null) {
				try {
					m_resizeListener.onResize(this, oldSize, newSize);
				} catch (Exception e) {
					log.error("Exception occurred while notifying resize listener.", e);
				}
			}
		}

		@Override
		public int pageSize() {
			return m_pageSize;
		}

		private void resize(int newSize) {
			if (newSize != m_size.get()) {
				boolean resize = false;
				int oldSize = m_size.get();
				synchronized (this) {
					if (newSize != m_size.get()) {
						LoadingCache<Long, Page<T>> newCache = buildCache(newSize);
						copyCacheDatas(m_cache.get(), newCache);
						oldSize = m_size.get();
						m_size.set(newSize);
						m_lastResizeTimeMillis.set(System.currentTimeMillis());
						Cache<Long, Boolean> recentlyExpiredPagesCache = buildRecentlyExpiredPagesCache();
						m_cache.set(newCache);
						m_recentlyExpiredPagesCache.set(recentlyExpiredPagesCache);
						resize = true;
					}
				}
				if (resize) {
					notifyResize(oldSize, newSize);
				}

			}
		}

		@Override
		public void shrink() {
			resize(m_coreSize);
		}

		@Override
		public int pageCount() {
			return m_size.get();
		}

		@Override
		public long getLastResizeTimeMillis() {
			return m_lastResizeTimeMillis.get();
		}

	}
}
