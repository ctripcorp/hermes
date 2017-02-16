package com.ctrip.hermes.broker.queue.storage.filter;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.codehaus.plexus.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.env.config.broker.BrokerConfigProvider;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

@Named(type = Filter.class)
public class DefaultFilter implements Filter, Initializable {
	private static final Logger log = LoggerFactory.getLogger(DefaultFilter.class);

	@Inject
	private BrokerConfigProvider m_config;

	private LoadingCache<String, Map<String, String>> m_filterConditionCache;

	private LoadingCache<String, Pattern> m_conditionPatternCache;

	private LoadingCache<String, LoadingCache<Pair<String, String>, Boolean>> m_matchedCache;

	private Map<String, String> parseConditions(String filter) {
		Map<String, String> map = new HashMap<>();
		try {
			StringBuilder sb = new StringBuilder(50);
			String k = null, v = null;
			char wordChar = 0;
			int idx = 0;
			while (idx < filter.length()) {
				wordChar = filter.charAt(idx);
				if (wordChar != ' ' && wordChar != '\t' && wordChar != '\n' && wordChar != ',') {
					break;
				}
				idx++;
			}
			boolean newCondition = true;
			while (idx < filter.length()) {
				char currrentChar = filter.charAt(idx);
				if (currrentChar != ' ' && currrentChar != '\t' && currrentChar != '\n') {
					wordChar = currrentChar;
					switch (wordChar) {
					case '~':
						k = sb.toString();
						sb = new StringBuilder(50);
						newCondition = false;
						break;
					case ',':
						v = sb.toString();
						sb = new StringBuilder(50);
						if (!newCondition) {
							map.put(k, v);
						}
						newCondition = true;
						break;
					default:
						sb.append(wordChar);
						break;
					}
				}
				idx++;
			}
			if (wordChar != ',') {
				v = sb.toString();
				map.put(k, v);
			}
		} catch (Exception e) {
			log.error("Parse filter failed: {}", filter, e);
		}
		return map;
	}

	private String buildMatchPattern(String topicPattern) {
		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < topicPattern.length(); i++) {
			if (i == 0) {
				sb.append("^");
			}

			char curChar = topicPattern.charAt(i);
			if (curChar == '*') {
				sb.append("[\\w-]+");
			} else if (curChar == '#') {
				sb.append("[\\w-\\.?]+");
			} else {
				sb.append(curChar);
			}

			if (i == topicPattern.length() - 1) {
				sb.append("$");
			}
		}

		return sb.toString();
	}

	@Override
	public boolean isMatch(String topicName, String filter, Map<String, String> source) {
		try {
			Map<String, String> conditions = m_filterConditionCache.get(filter);
			for (Entry<String, String> entry : conditions.entrySet()) {
				String sourceValue = source.get(entry.getKey());
				if (StringUtils.isBlank(sourceValue) || !matches(topicName, sourceValue, entry.getValue())) {
					return false;
				}
			}
		} catch (ExecutionException e) {
			log.error("Can not find matchedCache for topic: {]", topicName, e);
			return false;
		}
		return true;
	}

	private boolean isPattern(String str) {
		for (int i = 0; i < str.length(); i++) {
			if (str.charAt(i) == '*' || str.charAt(i) == '#') {
				return true;
			}
		}
		return false;
	}

	private boolean matches(String topic, String source, String pattern) {
		if (source.equalsIgnoreCase(pattern)) {
			return true;
		} else if (isPattern(pattern)) {
			try {
				return m_matchedCache.get(topic).get(new Pair<String, String>(source, pattern));
			} catch (ExecutionException e) {
				log.debug("Filter matched cache load failed: {}\t{}", source, pattern, e);
			}
		}
		return false;
	}

	@Override
	public void initialize() throws InitializationException {
		m_filterConditionCache = CacheBuilder.newBuilder() //
		      .initialCapacity(m_config.getFilterPatternCacheSize()) //
		      .maximumSize(m_config.getFilterPatternCacheSize()) //
		      .build(new CacheLoader<String, Map<String, String>>() {
			      @Override
			      public Map<String, String> load(String filter) throws Exception {
				      return parseConditions(filter);
			      }
		      });
		m_conditionPatternCache = CacheBuilder.newBuilder() //
		      .initialCapacity(m_config.getFilterPatternCacheSize()) //
		      .maximumSize(m_config.getFilterPatternCacheSize()) //
		      .build(new CacheLoader<String, Pattern>() {
			      @Override
			      public Pattern load(String condition) throws Exception {
				      return Pattern.compile(buildMatchPattern(condition), Pattern.CASE_INSENSITIVE);
			      }
		      });
		m_matchedCache = CacheBuilder.newBuilder() //
		      .initialCapacity(m_config.getFilterTopicCacheSize()) //
		      .maximumSize(m_config.getFilterTopicCacheSize()) //
		      .removalListener(new RemovalListener<String, LoadingCache<Pair<String, String>, Boolean>>() {
			      @Override
			      public void onRemoval(
			            RemovalNotification<String, LoadingCache<Pair<String, String>, Boolean>> notification) {
				      log.info("Topic: {}'s matched cache is removed: {}", notification.getKey(), notification.getCause());
			      }
		      }).build(new CacheLoader<String, LoadingCache<Pair<String, String>, Boolean>>() {
			      @Override
			      public LoadingCache<Pair<String, String>, Boolean> load(String key) throws Exception {
				      return CacheBuilder.newBuilder() //
				            .initialCapacity(m_config.getFilterPerTopicCacheSize()) //
				            .maximumSize(m_config.getFilterPerTopicCacheSize()) //
				            .build(new CacheLoader<Pair<String, String>, Boolean>() {
					            @Override
					            public Boolean load(Pair<String, String> key) throws Exception {
						            Pattern pattern = m_conditionPatternCache.get(key.getValue());
						            return pattern.matcher(key.getKey()).matches();
					            }
				            });
			      }
		      });
	}
}
