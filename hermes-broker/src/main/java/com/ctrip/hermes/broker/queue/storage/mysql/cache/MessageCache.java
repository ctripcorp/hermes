package com.ctrip.hermes.broker.queue.storage.mysql.cache;

import java.util.Collection;
import java.util.List;

import com.ctrip.hermes.broker.queue.storage.mysql.dal.IdAware;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface MessageCache<T extends IdAware> {
	public List<T> getOffsetAfter(String topic, int partition, long startOffsetExclusive, int batchSize);

	public interface MessageLoader<T extends IdAware> {
		public List<T> load(String topic, int partition, long startOffsetExclusive, int batchSize);
	}

	public interface ShrinkStrategy<T extends IdAware> {
		public int shrink(Collection<PageCache<T>> pageCaches);
	}

	public class DefaultShrinkStrategy<T extends IdAware> implements ShrinkStrategy<T> {

		private long m_shrinkAfterLastResizeTimeMillis;

		public DefaultShrinkStrategy(long shrinkAfterLastResizeTimeMillis) {
			m_shrinkAfterLastResizeTimeMillis = shrinkAfterLastResizeTimeMillis;
		}

		@Override
		public int shrink(Collection<PageCache<T>> pageCaches) {
			int count = 0;
			for (PageCache<T> pageCache : pageCaches) {
				if (System.currentTimeMillis() - pageCache.getLastResizeTimeMillis() > m_shrinkAfterLastResizeTimeMillis) {
					pageCache.shrink();
					count++;
				}
			}
			return count;
		}

	}
}
