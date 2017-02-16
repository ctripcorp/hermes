package com.ctrip.hermes.broker.status;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.RatioGauge;
import com.ctrip.hermes.broker.queue.storage.mysql.cache.PageCache;
import com.ctrip.hermes.metrics.HermesMetricsRegistry;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public enum BrokerStatusMonitor {
	INSTANCE;

	private static final String SAVED = "saved";
	private static final String AVG_BYTES = "avgBytes";
	private static final String _15_MIN = "15Min";
	private static final String _1_MIN = "1Min";
	private static final String _5_MIN = "5Min";
	private static final String MEAN = "Mean";
	private static final String RECEIVED = "received";
	private static final String DELIVERED = "Delivered";
	private static final String MESSAGE = "Message";
	private static final String CALLS = "calls";
	private static final String HAS_DATA = "hasData";
	private static final String HAS_DATA_RATIO = "hasDataRatio";
	private static final String PAGE_COUNT = "pageCount";
	private static final String PAGE_SIZE = "pageSize";
	private static final String CACHE = "cache";

	public void addCacheSizeGauge(final String topic, final int partition, String name, final PageCache<?> pageCache) {
		String pageSizeGaugeName = MetricRegistry.name(CACHE, name, PAGE_SIZE);
		HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).remove(pageSizeGaugeName);
		HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).register(pageSizeGaugeName, new Gauge<Integer>() {

			@Override
			public Integer getValue() {
				return pageCache.pageSize();
			}
		});
		String pageCountGuageName = MetricRegistry.name(CACHE, name, PAGE_COUNT);
		HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).remove(pageCountGuageName);
		HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).register(pageCountGuageName, new Gauge<Integer>() {

			@Override
			public Integer getValue() {
				return pageCache.pageCount();
			}
		});
		String hasDataRatio1MinName = MetricRegistry.name(CACHE, name, HAS_DATA_RATIO, _1_MIN);
		HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).remove(hasDataRatio1MinName);
		HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).register(hasDataRatio1MinName, new RatioGauge() {

			@Override
			protected Ratio getRatio() {
				Meter cacheCalls = HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).meter(
				      MetricRegistry.name(CACHE, CALLS));
				Meter cacheHits = HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).meter(
				      MetricRegistry.name(CACHE, HAS_DATA));
				return Ratio.of(cacheHits.getOneMinuteRate(), cacheCalls.getOneMinuteRate());
			}

		});
		String hasDataRatio5MinName = MetricRegistry.name(CACHE, name, HAS_DATA_RATIO, _5_MIN);
		HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).remove(hasDataRatio5MinName);
		HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).register(hasDataRatio5MinName, new RatioGauge() {

			@Override
			protected Ratio getRatio() {
				Meter cacheCalls = HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).meter(
				      MetricRegistry.name(CACHE, CALLS));
				Meter cacheHits = HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).meter(
				      MetricRegistry.name(CACHE, HAS_DATA));
				return Ratio.of(cacheHits.getFiveMinuteRate(), cacheCalls.getFiveMinuteRate());
			}

		});
		String hasDataRatio15MinName = MetricRegistry.name(CACHE, name, HAS_DATA_RATIO, _15_MIN);
		HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).remove(hasDataRatio15MinName);
		HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).register(hasDataRatio15MinName, new RatioGauge() {

			@Override
			protected Ratio getRatio() {
				Meter cacheCalls = HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).meter(
				      MetricRegistry.name(CACHE, CALLS));
				Meter cacheHits = HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).meter(
				      MetricRegistry.name(CACHE, HAS_DATA));
				return Ratio.of(cacheHits.getFifteenMinuteRate(), cacheCalls.getFifteenMinuteRate());
			}

		});
		String hasDataRatioMeanName = MetricRegistry.name(CACHE, name, HAS_DATA_RATIO, MEAN);
		HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).remove(hasDataRatioMeanName);
		HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).register(hasDataRatioMeanName, new RatioGauge() {

			@Override
			protected Ratio getRatio() {
				Meter cacheCalls = HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).meter(
				      MetricRegistry.name(CACHE, CALLS));
				Meter cacheHits = HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).meter(
				      MetricRegistry.name(CACHE, HAS_DATA));
				return Ratio.of(cacheHits.getMeanRate(), cacheCalls.getMeanRate());
			}

		});
	}

	public void cacheCall(String topic, int partition, boolean hasData) {
		HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).meter(MetricRegistry.name(CACHE, CALLS)).mark();
		if (hasData) {
			HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).meter(MetricRegistry.name(CACHE, HAS_DATA))
			      .mark();
		}
	}

	public void msgReceived(String topic, int partition, String clientIp, int totalBytes, int msgCount) {
		HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).meter(MetricRegistry.name(MESSAGE, RECEIVED))
		      .mark(msgCount);
		HermesMetricsRegistry.getMetricRegistryByTP(topic, partition)
		      .meter(MetricRegistry.name(MESSAGE, RECEIVED, clientIp)).mark(msgCount);
		HermesMetricsRegistry.getMetricRegistryByTP(topic, partition)
		      .histogram(MetricRegistry.name(MESSAGE, AVG_BYTES)).update(totalBytes / msgCount);
	}

	public void kafkaSend(String topic) {
		HermesMetricsRegistry.getMetricRegistryByT(topic).meter(MetricRegistry.name("KafkaStorage", "send", "meter"))
		      .mark();
	}

	public void msgSaved(String topic, int partition, int count) {
		HermesMetricsRegistry.getMetricRegistryByTP(topic, partition).meter(MetricRegistry.name(MESSAGE, SAVED))
		      .mark(count);
	}

	public void msgDelivered(String topic, int partition, String group, String clientIp, int msgCount) {
		HermesMetricsRegistry.getMetricRegistryByTPG(topic, partition, group)
		      .meter(MetricRegistry.name(MESSAGE, DELIVERED)).mark(msgCount);
		HermesMetricsRegistry.getMetricRegistryByTPG(topic, partition, group)
		      .meter(MetricRegistry.name(MESSAGE, DELIVERED, clientIp)).mark(msgCount);
	}
}
