package com.ctrip.hermes.broker.queue.storage.mysql.cache;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface PageCache<T> {
	public Page<T> get(long pageNo);

	public void shrink();

	public int pageCount();

	public int pageSize();

	public long getLastResizeTimeMillis();

	public interface ResizeListener<T> {
		public void onResize(PageCache<T> pageCache, int oldSize, int newSize);
	}

	public interface PageLoader<T> {
		public void loadPage(Page<T> page);
	}

}
