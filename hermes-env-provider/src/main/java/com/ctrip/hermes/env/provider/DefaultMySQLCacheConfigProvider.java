package com.ctrip.hermes.env.provider;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.tuple.Pair;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.ctrip.hermes.env.config.broker.MySQLCacheConfigProvider;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class DefaultMySQLCacheConfigProvider implements MySQLCacheConfigProvider {

	private static final Logger log = LoggerFactory.getLogger(DefaultMySQLCacheConfigProvider.class);

	private static final String FALSE = "false";

	private static final String BROKER_MYSQL_CACHE_ENABLE = "broker.mysql.cache.enable";

	private static final String BROKER_MYSQL_CACHE_ENABLED_TOPICS = "broker.mysql.cache.topics.enabled";

	private static final String BROKER_MYSQL_CACHE_DISABLED_TOPICS = "broker.mysql.cache.topics.disabled";

	private static final String BROKER_MYSQL_CACHE_SHRINK_AFTER_LAST_RESIZE_TIME_MILLIS = "broker.mysql.cache.shrinkAfterLastResize.timeMillis";

	private static final String BROKER_MYSQL_CACHE_PRIORITY_PAGE_CACHE_CORE_SIZE = "broker.mysql.cache.pageCache.default.coreSize.priority";

	private static final String BROKER_MYSQL_CACHE_PRIORITY_PAGE_CACHE_MAX_SIZE = "broker.mysql.cache.pageCache.default.maximumSize.priority";

	private static final String BROKER_MYSQL_CACHE_PRIORITY_PAGE_SIZE = "broker.mysql.cache.pageCache.default.pageSize.priority";

	private static final String BROKER_MYSQL_CACHE_PRIORITY_PAGE_LOAD_INTERVAL_MILLIS = "broker.mysql.cache.pageCache.page.load.interval.millis.priority";

	private static final String BROKER_MYSQL_CACHE_TOPIC_PRIORITY_PAGE_CACHE_SIZE = "broker.mysql.cache.pageCache.topic.priority";

	private static final String BROKER_MYSQL_CACHE_TOPIC_PRIORITY_PAGE_SIZE = "broker.mysql.cache.pageCache.topics.pageSize.priority";

	private static final String BROKER_MYSQL_CACHE_PRIORITY_MAX_CAPACITY = "broker.mysql.cache.maximumCapacity.priority";

	private static final String BROKER_MYSQL_CACHE_PRIORITY_CONCURRENCY_LEVEL = "broker.mysql.cache.concurrencyLevel.priority";

	private static final String BROKER_MYSQL_CACHE_NONPRIORITY_PAGE_CACHE_CORE_SIZE = "broker.mysql.cache.pageCache.default.coreSize.nonpriority";

	private static final String BROKER_MYSQL_CACHE_NONPRIORITY_PAGE_CACHE_MAX_SIZE = "broker.mysql.cache.pageCache.default.maximumSize.nonpriority";

	private static final String BROKER_MYSQL_CACHE_NONPRIORITY_PAGE_SIZE = "broker.mysql.cache.pageCache.default.pageSize.nonpriority";

	private static final String BROKER_MYSQL_CACHE_NONPRIORITY_PAGE_LOAD_INTERVAL_MILLIS = "broker.mysql.cache.pageCache.page.load.interval.millis.nonpriority";

	private static final String BROKER_MYSQL_CACHE_TOPIC_NONPRIORITY_PAGE_CACHE_SIZE = "broker.mysql.cache.pageCache.topic.nonpriority";

	private static final String BROKER_MYSQL_CACHE_TOPIC_NONPRIORITY_PAGE_SIZE = "broker.mysql.cache.pageCache.topics.pageSize.nonpriority";

	private static final String BROKER_MYSQL_CACHE_NONPRIORITY_MAX_CAPACITY = "broker.mysql.cache.maximumCapacity.nonpriority";

	private static final String BROKER_MYSQL_CACHE_NONPRIORITY_CONCURRENCY_LEVEL = "broker.mysql.cache.concurrencyLevel.nonpriority";

	private static final int DEFAULT_SHRINK_AFTER_LAST_RESIZE_TIME_MILLIS = 60 * 1000;

	private static final int DEFAULT_PRIORITY_PAGE_CACHE_CORE_SIZE = 1;

	private static final int DEFAULT_PRIORITY_PAGE_CACHE_MAXIMUM_SIZE = 8;

	private static final int DEFAULT_PRIORITY_PAGE_SIZE = 500;

	private static final int DEFAULT_PRIORITY_MESSAGE_MAXIMUM_CAPACITY = 1024 * 1024 * 2;

	private static final int DEFAULT_PRIORITY_PAGE_LOAD_INTERVAL_MILLIS = 20;

	private static final int DEFAULT_NONPRIORITY_PAGE_CACHE_CORE_SIZE = 1;

	private static final int DEFAULT_NONPRIORITY_PAGE_CACHE_MAXIMUM_SIZE = 16;

	private static final int DEFAULT_NONPRIORITY_PAGE_SIZE = 500;

	private static final int DEFAULT_NONPRIORITY_MESSAGE_MAXIMUM_CAPACITY = 1024 * 1024 * 4;

	private static final int DEFAULT_NONPRIORITY_PAGE_LOAD_INTERVAL_MILLIS = 20;

	private static final int DEFAULT_NONPRIORITY_MESSAGE_CONCURRENCY_LEVEL = 16;

	private static final int DEFAULT_PRIORITY_MESSAGE_CONCURRENCY_LEVEL = 16;

	private boolean m_enabled = true;

	private long m_shrinkAfterLastResizeTimeMillis = DEFAULT_SHRINK_AFTER_LAST_RESIZE_TIME_MILLIS;

	private final Set<Pattern> m_enabledTopicPatterns = new HashSet<>();

	private final Set<Pattern> m_disabledTopicPatterns = new HashSet<>();

	private final ConcurrentMap<String, Boolean> m_topicCacheSwitches = new ConcurrentHashMap<>();

	private int m_defaultPriorityPageCacheCoreSize = DEFAULT_PRIORITY_PAGE_CACHE_CORE_SIZE;

	private int m_defaultPriorityPageCacheMaximumSize = DEFAULT_PRIORITY_PAGE_CACHE_MAXIMUM_SIZE;

	private int m_defaultPriorityPageSize = DEFAULT_PRIORITY_PAGE_SIZE;

	private int m_priorityMessageMaximumCapacity = DEFAULT_PRIORITY_MESSAGE_MAXIMUM_CAPACITY;

	private int m_priorityMessageConcurrencyLevel = DEFAULT_PRIORITY_MESSAGE_CONCURRENCY_LEVEL;

	private int m_priorityPageLoadIntervalMillis = DEFAULT_PRIORITY_PAGE_LOAD_INTERVAL_MILLIS;

	private final Map<String, Pair<Integer, Integer>> m_topicPriorityPageCacheSizes = new HashMap<>();

	private final Map<String, Integer> m_topicPriorityPageSizes = new HashMap<>();

	private int m_defaultNonpriorityPageCacheCoreSize = DEFAULT_NONPRIORITY_PAGE_CACHE_CORE_SIZE;

	private int m_defaultNonpriorityPageCacheMaximumSize = DEFAULT_NONPRIORITY_PAGE_CACHE_MAXIMUM_SIZE;

	private int m_defaultNonpriorityPageSize = DEFAULT_NONPRIORITY_PAGE_SIZE;

	private int m_nonpriorityMessageMaximumCapacity = DEFAULT_NONPRIORITY_MESSAGE_MAXIMUM_CAPACITY;

	private int m_nonpriorityMessageConcurrencyLevel = DEFAULT_NONPRIORITY_MESSAGE_CONCURRENCY_LEVEL;

	private int m_nonpriorityPageLoadIntervalMillis = DEFAULT_NONPRIORITY_PAGE_LOAD_INTERVAL_MILLIS;

	private final Map<String, Pair<Integer, Integer>> m_topicNonpriorityPageCacheSizes = new HashMap<>();

	private final Map<String, Integer> m_topicNonpriorityPageSizes = new HashMap<>();

	public void init(Properties props) {
		parseEnabled(props);
		if (isEnabled()) {

			if (StringUtils.isNumeric(props.getProperty(BROKER_MYSQL_CACHE_SHRINK_AFTER_LAST_RESIZE_TIME_MILLIS))) {
				m_shrinkAfterLastResizeTimeMillis = Integer.valueOf(props
				      .getProperty(BROKER_MYSQL_CACHE_SHRINK_AFTER_LAST_RESIZE_TIME_MILLIS));
			}

			parseEnabledTopics(props.getProperty(BROKER_MYSQL_CACHE_ENABLED_TOPICS));
			parseDisabledTopics(props.getProperty(BROKER_MYSQL_CACHE_DISABLED_TOPICS));
			parsePriorityCacheConfig(props);
			parseNonpriorityCacheConfig(props);

			logAllConfiguration();
		}
	}

	private void logAllConfiguration() {
		log.info("shrinkAfterLastResizeTimeMillis : {}", m_shrinkAfterLastResizeTimeMillis);
		log.info("enabledTopicPatterns : {}", m_enabledTopicPatterns);
		log.info("disabledTopicPatterns : {}", m_disabledTopicPatterns);
		log.info("defaultPriorityPageCacheCoreSize : {}", m_defaultPriorityPageCacheCoreSize);
		log.info("defaultPriorityPageCacheMaximumSize : {}", m_defaultPriorityPageCacheMaximumSize);
		log.info("defaultPriorityPageSize : {}", m_defaultPriorityPageSize);
		log.info("priorityMessageMaximumCapacity : {}", m_priorityMessageMaximumCapacity);
		log.info("priorityMessageConcurrencyLevel : {}", m_priorityMessageConcurrencyLevel);
		log.info("priorityPageLoadIntervalMillis : {}", m_priorityPageLoadIntervalMillis);
		log.info("topicPriorityPageCacheSizes : {}", m_topicPriorityPageCacheSizes);
		log.info("topicPriorityPageSizes : {}", m_topicPriorityPageSizes);
		log.info("defaultNonpriorityPageCacheCoreSize : {}", m_defaultNonpriorityPageCacheCoreSize);
		log.info("defaultNonpriorityPageCacheMaximumSize : {}", m_defaultNonpriorityPageCacheMaximumSize);
		log.info("defaultNonpriorityPageSize : {}", m_defaultNonpriorityPageSize);
		log.info("nonpriorityMessageMaximumCapacity : {}", m_nonpriorityMessageMaximumCapacity);
		log.info("nonpriorityMessageConcurrencyLevel : {}", m_nonpriorityMessageConcurrencyLevel);
		log.info("nonpriorityPageLoadIntervalMillis : {}", m_nonpriorityPageLoadIntervalMillis);
		log.info("topicNonpriorityPageCacheSizes : {}", m_topicNonpriorityPageCacheSizes);
		log.info("topicNonpriorityPageSizes : {}", m_topicNonpriorityPageSizes);
	}

	private void parsePriorityCacheConfig(Properties props) {
		if (StringUtils.isNumeric(props.getProperty(BROKER_MYSQL_CACHE_PRIORITY_PAGE_CACHE_CORE_SIZE))) {
			m_defaultPriorityPageCacheCoreSize = Integer.valueOf(props
			      .getProperty(BROKER_MYSQL_CACHE_PRIORITY_PAGE_CACHE_CORE_SIZE));
		}
		if (StringUtils.isNumeric(props.getProperty(BROKER_MYSQL_CACHE_PRIORITY_PAGE_CACHE_MAX_SIZE))) {
			m_defaultPriorityPageCacheMaximumSize = Integer.valueOf(props
			      .getProperty(BROKER_MYSQL_CACHE_PRIORITY_PAGE_CACHE_MAX_SIZE));
		}
		if (StringUtils.isNumeric(props.getProperty(BROKER_MYSQL_CACHE_PRIORITY_PAGE_SIZE))) {
			m_defaultPriorityPageSize = Integer.valueOf(props.getProperty(BROKER_MYSQL_CACHE_PRIORITY_PAGE_SIZE));
		}
		if (StringUtils.isNumeric(props.getProperty(BROKER_MYSQL_CACHE_PRIORITY_MAX_CAPACITY))) {
			m_priorityMessageMaximumCapacity = Integer
			      .valueOf(props.getProperty(BROKER_MYSQL_CACHE_PRIORITY_MAX_CAPACITY));
		}
		if (StringUtils.isNumeric(props.getProperty(BROKER_MYSQL_CACHE_PRIORITY_PAGE_LOAD_INTERVAL_MILLIS))) {
			m_priorityPageLoadIntervalMillis = Integer.valueOf(props
			      .getProperty(BROKER_MYSQL_CACHE_PRIORITY_PAGE_LOAD_INTERVAL_MILLIS));
		}
		if (StringUtils.isNumeric(props.getProperty(BROKER_MYSQL_CACHE_PRIORITY_CONCURRENCY_LEVEL))) {
			m_priorityMessageConcurrencyLevel = Integer.valueOf(props
			      .getProperty(BROKER_MYSQL_CACHE_PRIORITY_CONCURRENCY_LEVEL));
		}
		if (!StringUtils.isBlank(props.getProperty(BROKER_MYSQL_CACHE_TOPIC_PRIORITY_PAGE_CACHE_SIZE))) {
			try {
				Map<String, List<Integer>> topicPriorityPageCacheSizes = JSON.parseObject(
				      props.getProperty(BROKER_MYSQL_CACHE_TOPIC_PRIORITY_PAGE_CACHE_SIZE),
				      new TypeReference<Map<String, List<Integer>>>() {
				      });

				if (topicPriorityPageCacheSizes != null) {
					for (Map.Entry<String, List<Integer>> entry : topicPriorityPageCacheSizes.entrySet()) {
						if (entry.getValue() != null && entry.getValue().size() == 2) {
							m_topicPriorityPageCacheSizes.put(entry.getKey(),
							      new Pair<Integer, Integer>(entry.getValue().get(0), entry.getValue().get(1)));
							log.info("Topic [{}] specified page cache's core size {} and maximum size {}(priority)",
							      entry.getKey(), entry.getValue().get(0), entry.getValue().get(1));
						}
					}
				}

			} catch (Exception e) {
				log.error("Parse topic specified page cache size failed.", e);
			}
		}
		if (!StringUtils.isBlank(props.getProperty(BROKER_MYSQL_CACHE_TOPIC_PRIORITY_PAGE_SIZE))) {
			try {
				Map<String, Integer> topicPriorityPageSizes = JSON.parseObject(
				      props.getProperty(BROKER_MYSQL_CACHE_TOPIC_PRIORITY_PAGE_SIZE),
				      new TypeReference<Map<String, Integer>>() {
				      });

				if (topicPriorityPageSizes != null) {
					for (Map.Entry<String, Integer> entry : topicPriorityPageSizes.entrySet()) {
						m_topicPriorityPageSizes.put(entry.getKey(), entry.getValue());
						log.info("Topic [{}] specified page cache's page size {}(priority)", entry.getKey(), entry.getValue());
					}
				}

			} catch (Exception e) {
				log.error("Parse topic specified page cache's page size failed.", e);
			}
		}
	}

	private void parseNonpriorityCacheConfig(Properties props) {
		if (StringUtils.isNumeric(props.getProperty(BROKER_MYSQL_CACHE_NONPRIORITY_PAGE_CACHE_CORE_SIZE))) {
			m_defaultNonpriorityPageCacheCoreSize = Integer.valueOf(props
			      .getProperty(BROKER_MYSQL_CACHE_NONPRIORITY_PAGE_CACHE_CORE_SIZE));
		}
		if (StringUtils.isNumeric(props.getProperty(BROKER_MYSQL_CACHE_NONPRIORITY_PAGE_CACHE_MAX_SIZE))) {
			m_defaultNonpriorityPageCacheMaximumSize = Integer.valueOf(props
			      .getProperty(BROKER_MYSQL_CACHE_NONPRIORITY_PAGE_CACHE_MAX_SIZE));
		}
		if (StringUtils.isNumeric(props.getProperty(BROKER_MYSQL_CACHE_NONPRIORITY_PAGE_SIZE))) {
			m_defaultNonpriorityPageSize = Integer.valueOf(props.getProperty(BROKER_MYSQL_CACHE_NONPRIORITY_PAGE_SIZE));
		}
		if (StringUtils.isNumeric(props.getProperty(BROKER_MYSQL_CACHE_NONPRIORITY_MAX_CAPACITY))) {
			m_nonpriorityMessageMaximumCapacity = Integer.valueOf(props
			      .getProperty(BROKER_MYSQL_CACHE_NONPRIORITY_MAX_CAPACITY));
		}
		if (StringUtils.isNumeric(props.getProperty(BROKER_MYSQL_CACHE_NONPRIORITY_PAGE_LOAD_INTERVAL_MILLIS))) {
			m_nonpriorityPageLoadIntervalMillis = Integer.valueOf(props
			      .getProperty(BROKER_MYSQL_CACHE_NONPRIORITY_PAGE_LOAD_INTERVAL_MILLIS));
		}
		if (StringUtils.isNumeric(props.getProperty(BROKER_MYSQL_CACHE_NONPRIORITY_CONCURRENCY_LEVEL))) {
			m_nonpriorityMessageConcurrencyLevel = Integer.valueOf(props
			      .getProperty(BROKER_MYSQL_CACHE_NONPRIORITY_CONCURRENCY_LEVEL));
		}
		if (!StringUtils.isBlank(props.getProperty(BROKER_MYSQL_CACHE_TOPIC_NONPRIORITY_PAGE_CACHE_SIZE))) {
			try {
				Map<String, List<Integer>> topicNonpriorityPageCacheSizes = JSON.parseObject(
				      props.getProperty(BROKER_MYSQL_CACHE_TOPIC_NONPRIORITY_PAGE_CACHE_SIZE),
				      new TypeReference<Map<String, List<Integer>>>() {
				      });

				if (topicNonpriorityPageCacheSizes != null) {
					for (Map.Entry<String, List<Integer>> entry : topicNonpriorityPageCacheSizes.entrySet()) {
						if (entry.getValue() != null && entry.getValue().size() == 2) {
							m_topicNonpriorityPageCacheSizes.put(entry.getKey(), new Pair<Integer, Integer>(entry.getValue()
							      .get(0), entry.getValue().get(1)));
							log.info("Topic [{}] specified page cache's core size {} and maximum size {}(nonpriority)",
							      entry.getKey(), entry.getValue().get(0), entry.getValue().get(1));
						}
					}
				}

			} catch (Exception e) {
				log.error("Parse topic specified page cache size failed.", e);
			}
		}
		if (!StringUtils.isBlank(props.getProperty(BROKER_MYSQL_CACHE_TOPIC_NONPRIORITY_PAGE_SIZE))) {
			try {
				Map<String, Integer> topicNonpriorityPageSizes = JSON.parseObject(
				      props.getProperty(BROKER_MYSQL_CACHE_TOPIC_NONPRIORITY_PAGE_SIZE),
				      new TypeReference<Map<String, Integer>>() {
				      });

				if (topicNonpriorityPageSizes != null) {
					for (Map.Entry<String, Integer> entry : topicNonpriorityPageSizes.entrySet()) {
						m_topicNonpriorityPageSizes.put(entry.getKey(), entry.getValue());
						log.info("Topic [{}] specified page cache's page size {}(nonpriority)", entry.getKey(),
						      entry.getValue());
					}
				}

			} catch (Exception e) {
				log.error("Parse topic specified page cache's page size failed.", e);
			}
		}
	}

	private void parseEnabledTopics(String content) {
		if (!StringUtils.isBlank(content)) {
			try {
				Set<String> enabledTopicRegexs = JSON.parseObject(content, new TypeReference<Set<String>>() {
				});

				if (enabledTopicRegexs != null) {
					for (String regex : enabledTopicRegexs) {
						m_enabledTopicPatterns.add(Pattern.compile(regex));
						log.info("Topic pattern [{}] enabled cache", regex);
					}
				}

			} catch (Exception e) {
				log.error("Parse enabled topics failed.", e);
			}

		}
	}

	private void parseDisabledTopics(String content) {
		if (!StringUtils.isBlank(content)) {
			try {
				Set<String> disabledTopicRegexs = JSON.parseObject(content, new TypeReference<Set<String>>() {
				});

				if (disabledTopicRegexs != null) {
					for (String regex : disabledTopicRegexs) {
						m_disabledTopicPatterns.add(Pattern.compile(regex));
						log.info("Topic pattern [{}] disabled cache", regex);
					}
				}

			} catch (Exception e) {
				log.error("Parse disabled topics failed.", e);
			}
		}
	}

	private void parseEnabled(Properties props) {
		String enable = System.getProperty(BROKER_MYSQL_CACHE_ENABLE);
		if (!StringUtils.isBlank(enable)) {
			m_enabled = !FALSE.equalsIgnoreCase(enable);
		} else {
			m_enabled = !FALSE.equalsIgnoreCase(props.getProperty(BROKER_MYSQL_CACHE_ENABLE, Boolean.TRUE.toString()));
		}

		log.info("Cache enabled: {}", m_enabled);
	}

	public boolean isEnabled() {
		return m_enabled;
	}

	public long shrinkAfterLastResizeTimeMillis() {
		return m_shrinkAfterLastResizeTimeMillis;
	}

	public boolean shouldCache(String topic) {
		if (!isEnabled()) {
			return false;
		} else {
			Boolean enabled = m_topicCacheSwitches.get(topic);
			if (enabled == null) {
				synchronized (m_topicCacheSwitches) {
					enabled = m_topicCacheSwitches.get(topic);
					if (enabled == null) {
						enabled = isCacheEnabled(topic);
						m_topicCacheSwitches.put(topic, enabled);
					}
				}
			}

			return enabled;
		}
	}

	private boolean isCacheEnabled(String topic) {
		for (Pattern disabledTopicPattern : m_disabledTopicPatterns) {
			if (disabledTopicPattern.matcher(topic).matches()) {
				return false;
			}
		}
		for (Pattern enabledTopicPattern : m_enabledTopicPatterns) {
			if (enabledTopicPattern.matcher(topic).matches()) {
				return true;
			}
		}
		return false;
	}

	public int defaultPriorityPageCacheCoreSize() {
		return m_defaultPriorityPageCacheCoreSize;
	}

	public int defaultPriorityPageCacheMaximumSize() {
		return m_defaultPriorityPageCacheMaximumSize;
	}

	public int defaultPriorityPageSize() {
		return m_defaultPriorityPageSize;
	}

	public int priorityMessageMaximumCapacity() {
		return m_priorityMessageMaximumCapacity;
	}

	public int priorityPageLoadIntervalMillis() {
		return m_priorityPageLoadIntervalMillis;
	}

	public int priorityMessageConcurrencyLevel() {
		return m_priorityMessageConcurrencyLevel;
	}

	public Map<String, Pair<Integer, Integer>> topicPriorityPageCacheSizes() {
		return m_topicPriorityPageCacheSizes;
	}

	public Map<String, Integer> topicPriorityPageSizes() {
		return m_topicPriorityPageSizes;
	}

	public int defaultNonpriorityPageCacheCoreSize() {
		return m_defaultNonpriorityPageCacheCoreSize;
	}

	public int defaultNonpriorityPageCacheMaximumSize() {
		return m_defaultNonpriorityPageCacheMaximumSize;
	}

	public int defaultNonpriorityPageSize() {
		return m_defaultNonpriorityPageSize;
	}

	public int nonpriorityMessageMaximumCapacity() {
		return m_nonpriorityMessageMaximumCapacity;
	}

	public int nonpriorityPageLoadIntervalMillis() {
		return m_nonpriorityPageLoadIntervalMillis;
	}

	public int nonpriorityMessageConcurrencyLevel() {
		return m_nonpriorityMessageConcurrencyLevel;
	}

	public Map<String, Pair<Integer, Integer>> topicNonpriorityPageCacheSizes() {
		return m_topicNonpriorityPageCacheSizes;
	}

	public Map<String, Integer> topicNonpriorityPageSizes() {
		return m_topicNonpriorityPageSizes;
	}

}
