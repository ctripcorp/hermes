package com.ctrip.hermes.broker.queue.storage.mysql.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.broker.queue.storage.mysql.cache.PageCache.PageLoader;
import com.ctrip.hermes.broker.queue.storage.mysql.cache.PageCache.ResizeListener;
import com.ctrip.hermes.broker.queue.storage.mysql.cache.PageCacheBuilder.DefaultPageCache;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class DefaultPageCacheTest {

	@Test
	public void testNoResize() throws Exception {
		final AtomicBoolean resize = new AtomicBoolean(false);

		int size = 3;

		DefaultPageCache<Long> cache = createCache(size, size, 1, 0, new ResizeListener<Long>() {

			@Override
			public void onResize(PageCache<Long> pageCache, int oldSize, int newSize) {
				resize.set(true);
			}
		}, null);

		int getCount = 1000;

		for (int i = 0; i < getCount; i++) {
			cache.get(i);
		}

		assertEquals(size, cache.pageCount());
		assertFalse(resize.get());
		for (long i = 0; i < getCount; i++) {
			long pageNo = getCount - 1 - i;

			if (i < size) {
				assertTrue(cache.contains(pageNo));
				Page<Long> page = cache.get(pageNo);
				List<Long> datas = page.getDatas(page.getStartOffset(), 1);
				assertEquals(1, datas.size());
				assertTrue(datas.contains(pageNo));
			} else {
				assertFalse(cache.contains(pageNo));
			}
		}
	}

	@Test
	public void testExpandAndShrink() throws Exception {
		final AtomicInteger expandCount = new AtomicInteger(0);
		final AtomicInteger shrinkCount = new AtomicInteger(0);

		DefaultPageCache<Long> cache = createCache(1, 3, 1, 0, new ResizeListener<Long>() {

			@Override
			public void onResize(PageCache<Long> pageCache, int oldSize, int newSize) {
				if (newSize > oldSize) {
					expandCount.incrementAndGet();
				} else {
					shrinkCount.incrementAndGet();
				}
			}
		}, null);

		// expand
		cache.get(1);
		assertEquals(1, cache.pageCount());
		assertTrue(cache.contains(1));

		cache.get(2);
		assertEquals(1, cache.pageCount());
		assertTrue(cache.contains(2));

		cache.get(1);
		assertEquals(2, cache.pageCount());
		assertTrue(cache.contains(1));
		assertFalse(cache.contains(2));
		assertTrue(Math.abs(System.currentTimeMillis() - cache.getLastResizeTimeMillis()) < 10L);

		cache.get(2);
		assertEquals(2, cache.pageCount());
		assertTrue(cache.contains(1));
		assertTrue(cache.contains(2));

		cache.get(3);
		assertEquals(2, cache.pageCount());
		assertTrue(cache.contains(2));
		assertTrue(cache.contains(3));
		assertFalse(cache.contains(1));

		cache.get(1);
		assertEquals(3, cache.pageCount());
		assertTrue(cache.contains(3));
		assertTrue(cache.contains(1));
		assertFalse(cache.contains(2));
		assertTrue(Math.abs(System.currentTimeMillis() - cache.getLastResizeTimeMillis()) < 10L);

		cache.get(4);
		assertEquals(3, cache.pageCount());
		assertTrue(cache.contains(3));
		assertTrue(cache.contains(1));
		assertTrue(cache.contains(4));

		cache.get(5);
		assertEquals(3, cache.pageCount());
		assertTrue(cache.contains(1));
		assertTrue(cache.contains(4));
		assertTrue(cache.contains(5));

		cache.shrink();
		assertEquals(1, cache.pageCount());
		assertTrue(cache.contains(5));
		assertFalse(cache.contains(1));
		assertFalse(cache.contains(4));
		assertTrue(Math.abs(System.currentTimeMillis() - cache.getLastResizeTimeMillis()) < 10L);

		assertEquals(2, expandCount.get());
		assertEquals(1, shrinkCount.get());
	}

	@Test
	public void testPageLoadInterval() throws Exception {
		DefaultPageCache<Long> cache = createCache(1, 1, 2, 50, null, new PageLoader<Long>() {

			AtomicLong m_idGenerator = new AtomicLong(0);

			@Override
			public void loadPage(Page<Long> page) {
				page.addData(Arrays.asList(new Pair<>(page.getLatestLoadedOffset() + 1, m_idGenerator.incrementAndGet())));
			}
		});

		long start = System.currentTimeMillis();
		Page<Long> page = cache.get(0);
		List<Long> datas = page.getDatas(0, 2);
		assertTrue(datas.isEmpty());

		int step = 0;

		while (true) {
			page = cache.get(0);
			datas = page.getDatas(0, 2);
			if (datas.isEmpty()) {
				continue;
			} else {
				if (step == 0) {
					assertEquals(1, datas.size());
					assertTrue(datas.contains(1L));
					assertFalse(page.isComplete());
					step = 1;
				} else {
					if (datas.size() == 2) {
						assertTrue(datas.contains(1L));
						assertTrue(datas.contains(2L));
						assertTrue(page.isComplete());
						assertTrue(System.currentTimeMillis() - start >= 50L);
						break;
					}
				}
			}
		}
	}

	private DefaultPageCache<Long> createCache(int coreSize, int maxSize, int pageSize, int pageLoadIntervalMillis,
	      ResizeListener<Long> resizeListener, PageLoader<Long> pageLoader) {

		PageCacheBuilder builder = PageCacheBuilder.newBuilder()//
		      .pageSize(pageSize)//
		      .coreSize(coreSize)//
		      .maxSize(maxSize)//
		      .pageLoadIntervalMillis(pageLoadIntervalMillis)//
		      .pageLoaderThreadPool(Executors.newCachedThreadPool())//
		      .resizeListener(resizeListener);
		if (pageLoader != null) {
			builder.pageLoader(pageLoader);
		} else {
			builder.pageLoader(new PageLoader<Long>() {

				@Override
				public void loadPage(Page<Long> page) {
					page.addData(Arrays.asList(new Pair<Long, Long>(page.getStartOffset(), page.getStartOffset())));
				}
			});
		}
		PageCache<Long> cache = builder.build();
		return (DefaultPageCache<Long>) cache;
	}
}
