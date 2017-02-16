package com.ctrip.hermes.broker.queue.storage.mysql.dal;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.broker.dal.hermes.MessagePriority;
import com.ctrip.hermes.broker.dal.hermes.MessagePriorityDao;
import com.ctrip.hermes.broker.dal.hermes.MessagePriorityEntity;
import com.ctrip.hermes.broker.queue.storage.mysql.cache.MessageCache;
import com.ctrip.hermes.broker.queue.storage.mysql.cache.MessageCache.DefaultShrinkStrategy;
import com.ctrip.hermes.broker.queue.storage.mysql.cache.MessageCache.MessageLoader;
import com.ctrip.hermes.broker.queue.storage.mysql.cache.MessageCacheBuilder;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.env.config.broker.MySQLCacheConfigProvider;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class CachedMessagePriorityDaoInterceptor implements MethodInterceptor {

	private static final Logger log = LoggerFactory.getLogger(CachedMessagePriorityDaoInterceptor.class);

	private MessagePriorityDao m_target;

	private MessageCache<MessagePriority> m_priorityMessageCache;

	private MessageCache<MessagePriority> m_nonpriorityMessageCache;

	private MySQLCacheConfigProvider m_config;

	public static MessagePriorityDao createProxy(MessagePriorityDao target, MySQLCacheConfigProvider config) {
		CachedMessagePriorityDaoInterceptor proxy = new CachedMessagePriorityDaoInterceptor(target, config);
		Enhancer enhancer = new Enhancer();
		enhancer.setSuperclass(target.getClass());
		enhancer.setCallback(proxy);
		enhancer.setClassLoader(target.getClass().getClassLoader());
		return (MessagePriorityDao) enhancer.create();
	}

	private CachedMessagePriorityDaoInterceptor(MessagePriorityDao target, MySQLCacheConfigProvider config) {
		m_target = target;
		m_config = config;

		initCache();
	}

	private void initCache() {

		initPriorityMessageCache();

		initNonpriorityMessageCache();
	}

	private void initNonpriorityMessageCache() {
		MessageCacheBuilder nonpriorityCacheBuilder = MessageCacheBuilder.newBuilder()//
		      .concurrencyLevel(m_config.nonpriorityMessageConcurrencyLevel())//
		      .defaultPageCacheCoreSize(m_config.defaultNonpriorityPageCacheCoreSize())//
		      .defaultPageCacheMaximumSize(m_config.defaultNonpriorityPageCacheMaximumSize())//
		      .defaultPageSize(m_config.defaultNonpriorityPageSize())//
		      .maximumMessageCapacity(m_config.nonpriorityMessageMaximumCapacity())//
		      .pageLoadIntervalMillis(m_config.nonpriorityPageLoadIntervalMillis())//
		      .name("nonpriority")//
		      .shrinkStrategy(new DefaultShrinkStrategy<MessagePriority>(m_config.shrinkAfterLastResizeTimeMillis()))//
		      .messageLoader(new MessageLoader<MessagePriority>() {

			      @Override
			      public List<MessagePriority> load(String topic, int partition, long startOffsetExclusive, int batchSize) {
				      return loadMessages(topic, partition, 1, startOffsetExclusive, batchSize);
			      }
		      });

		if (m_config.topicNonpriorityPageCacheSizes() != null) {
			for (Map.Entry<String, Pair<Integer, Integer>> entry : m_config.topicNonpriorityPageCacheSizes().entrySet()) {
				nonpriorityCacheBuilder.topicPageCacheSize(entry.getKey(), entry.getValue().getKey(), entry.getValue()
				      .getValue());
			}
		}
		if (m_config.topicNonpriorityPageSizes() != null) {
			for (Map.Entry<String, Integer> entry : m_config.topicNonpriorityPageSizes().entrySet()) {
				nonpriorityCacheBuilder.topicPageSize(entry.getKey(), entry.getValue());
			}
		}

		m_nonpriorityMessageCache = nonpriorityCacheBuilder.build();
	}

	private void initPriorityMessageCache() {
		MessageCacheBuilder priorityCacheBuilder = MessageCacheBuilder.newBuilder()//
		      .concurrencyLevel(m_config.priorityMessageConcurrencyLevel())//
		      .defaultPageCacheCoreSize(m_config.defaultPriorityPageCacheCoreSize())//
		      .defaultPageCacheMaximumSize(m_config.defaultPriorityPageCacheMaximumSize())//
		      .defaultPageSize(m_config.defaultPriorityPageSize())//
		      .maximumMessageCapacity(m_config.priorityMessageMaximumCapacity())//
		      .pageLoadIntervalMillis(m_config.priorityPageLoadIntervalMillis())//
		      .name("priority")//
		      .shrinkStrategy(new DefaultShrinkStrategy<MessagePriority>(m_config.shrinkAfterLastResizeTimeMillis()))//
		      .messageLoader(new MessageLoader<MessagePriority>() {

			      @Override
			      public List<MessagePriority> load(String topic, int partition, long startOffsetExclusive, int batchSize) {
				      return loadMessages(topic, partition, 0, startOffsetExclusive, batchSize);
			      }

		      });

		if (m_config.topicPriorityPageCacheSizes() != null) {
			for (Map.Entry<String, Pair<Integer, Integer>> entry : m_config.topicPriorityPageCacheSizes().entrySet()) {
				priorityCacheBuilder.topicPageCacheSize(entry.getKey(), entry.getValue().getKey(), entry.getValue()
				      .getValue());
			}
		}
		if (m_config.topicPriorityPageSizes() != null) {
			for (Map.Entry<String, Integer> entry : m_config.topicPriorityPageSizes().entrySet()) {
				priorityCacheBuilder.topicPageSize(entry.getKey(), entry.getValue());
			}
		}

		m_priorityMessageCache = priorityCacheBuilder.build();
	}

	private List<MessagePriority> loadMessages(String topic, int partition, int priorityInt, long startOffsetExclusive,
	      int batchSize) {
		try {
			return m_target.findIdAfter(topic, partition, priorityInt, startOffsetExclusive, batchSize,
			      MessagePriorityEntity.READSET_FULL);
		} catch (DalException e) {
			log.error("Failed to fetch message({}).", new Tpp(topic, partition, true), e);
			return null;
		}
	}

	@Override
	public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
		if ("findIdAfter".equals(method.getName())) {
			String topic = (String) args[0];
			if (m_config.shouldCache(topic)) {
				int partition = (int) args[1];
				boolean isPriority = ((int) args[2]) == 0 ? true : false;

				MessageCache<MessagePriority> cache = isPriority ? m_priorityMessageCache : m_nonpriorityMessageCache;

				return cache.getOffsetAfter(topic, partition, (long) args[3], (int) args[4]);
			}
		}
		return method.invoke(m_target, args);
	}

}
