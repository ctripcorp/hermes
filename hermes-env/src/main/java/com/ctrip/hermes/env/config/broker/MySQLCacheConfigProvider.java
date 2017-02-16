package com.ctrip.hermes.env.config.broker;

import java.util.Map;

import org.unidal.tuple.Pair;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface MySQLCacheConfigProvider {

	public boolean isEnabled();

	public long shrinkAfterLastResizeTimeMillis();

	public boolean shouldCache(String topic);

	public int defaultPriorityPageCacheCoreSize();

	public int defaultPriorityPageCacheMaximumSize();

	public int defaultPriorityPageSize();

	public int priorityMessageMaximumCapacity();

	public int priorityPageLoadIntervalMillis();

	public int priorityMessageConcurrencyLevel();

	public Map<String, Pair<Integer, Integer>> topicPriorityPageCacheSizes();

	public Map<String, Integer> topicPriorityPageSizes();

	public int defaultNonpriorityPageCacheCoreSize();

	public int defaultNonpriorityPageCacheMaximumSize();

	public int defaultNonpriorityPageSize();

	public int nonpriorityMessageMaximumCapacity();

	public int nonpriorityPageLoadIntervalMillis();

	public int nonpriorityMessageConcurrencyLevel();

	public Map<String, Pair<Integer, Integer>> topicNonpriorityPageCacheSizes();

	public Map<String, Integer> topicNonpriorityPageSizes();
}
