package com.ctrip.hermes.metrics;

import java.util.HashMap;
import java.util.Map;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;

public class HermesMetricsRegistry {

	private static Map<String, MetricRegistry> t_metrics = new HashMap<String, MetricRegistry>();

	private static Map<String, MetricRegistry> tp_metrics = new HashMap<String, MetricRegistry>();

	private static Map<String, MetricRegistry> tpg_metrics = new HashMap<String, MetricRegistry>();

	private static MetricRegistry default_metrics = new MetricRegistry();

	private static HealthCheckRegistry healthCheckRegistry = new HealthCheckRegistry();

	private static Map<String, JmxReporter> reporters = new HashMap<String, JmxReporter>();

	public static void close() {
		for (JmxReporter reporter : reporters.values()) {
			reporter.close();
		}

		default_metrics.removeMatching(new MetricFilter() {

			@Override
			public boolean matches(String name, Metric metric) {
				return true;
			}

		});

		for (MetricRegistry metricRegistry : t_metrics.values()) {
			metricRegistry.removeMatching(new MetricFilter() {

				@Override
				public boolean matches(String name, Metric metric) {
					return true;
				}

			});
		}

		for (MetricRegistry metricRegistry : tp_metrics.values()) {
			metricRegistry.removeMatching(new MetricFilter() {

				@Override
				public boolean matches(String name, Metric metric) {
					return true;
				}

			});
		}

		for (MetricRegistry metricRegistry : tpg_metrics.values()) {
			metricRegistry.removeMatching(new MetricFilter() {

				@Override
				public boolean matches(String name, Metric metric) {
					return true;
				}

			});
		}
	}

	public static HealthCheckRegistry getHealthCheckRegistry() {
		return healthCheckRegistry;
	}

	/**
	 * 
	 * @return
	 */
	static Map<String, MetricRegistry> getMetricRegistiesGroupByT() {
		return t_metrics;
	}

	/**
	 * 
	 * @return
	 */
	static Map<String, MetricRegistry> getMetricRegistiesGroupByTP() {
		return tp_metrics;
	}

	/**
	 * 
	 * @return
	 */
	static Map<String, MetricRegistry> getMetricRegistiesGroupByTPG() {
		return tpg_metrics;
	}

	/**
	 * 
	 * @return
	 */
	public static MetricRegistry getMetricRegistry() {
		return default_metrics;
	}

	/**
	 * 
	 * @param topic
	 * @return
	 */
	public static MetricRegistry getMetricRegistryByT(String topic) {
		if (!t_metrics.containsKey(topic)) {
			synchronized (t_metrics) {
				if (!t_metrics.containsKey(topic)) {
					MetricRegistry metricRegistry = new MetricRegistry();
					t_metrics.put(topic, metricRegistry);
					JmxReporter reporter = JmxReporter.forRegistry(metricRegistry).inDomain(topic).build();
					reporter.start();
					reporters.put(topic, reporter);
				}
			}
		}
		return t_metrics.get(topic);
	}

	/**
	 * 
	 * @param topic
	 * @param partition
	 * @return
	 */
	public static MetricRegistry getMetricRegistryByTP(String topic, int partition) {
		String key = topic + "|" + partition;
		if (!tp_metrics.containsKey(key)) {
			synchronized (tp_metrics) {
				if (!tp_metrics.containsKey(key)) {
					MetricRegistry metricRegistry = new MetricRegistry();
					tp_metrics.put(key, metricRegistry);
					JmxReporter reporter = JmxReporter.forRegistry(metricRegistry).inDomain(key).build();
					reporter.start();
					reporters.put(key, reporter);
				}
			}
		}
		return tp_metrics.get(key);
	}

	/**
	 * 
	 * @param topic
	 * @param partition
	 * @param group
	 * @return
	 */
	public static MetricRegistry getMetricRegistryByTPG(String topic, int partition, String group) {
		String key = topic + "|" + partition + "|" + group;
		if (!tpg_metrics.containsKey(key)) {
			synchronized (tpg_metrics) {
				if (!tpg_metrics.containsKey(key)) {
					MetricRegistry metricRegistry = new MetricRegistry();
					tpg_metrics.put(key, metricRegistry);
					JmxReporter reporter = JmxReporter.forRegistry(metricRegistry).inDomain(key).build();
					reporter.start();
					reporters.put(key, reporter);
				}
			}
		}
		return tpg_metrics.get(key);
	}

	public static void reset() {
		close();
		t_metrics.clear();
		tp_metrics.clear();
		tpg_metrics.clear();
		for (JmxReporter reporter : reporters.values()) {
			reporter.close();
		}
		reporters.clear();
		healthCheckRegistry = new HealthCheckRegistry();
	}
}
