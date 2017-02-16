package com.ctrip.hermes.metrics;

import java.lang.management.ManagementFactory;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.ClassLoadingGaugeSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import com.codahale.metrics.servlets.MetricsServlet;

public class JVMMetricsServletContextListener extends JVMMetricsServlet.ContextListener {

	public static final MetricRegistry JVM_METRIC_REGISTRY = new MetricRegistry();

	static {
		JVM_METRIC_REGISTRY.registerAll(new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer()));
		JVM_METRIC_REGISTRY.registerAll(new ClassLoadingGaugeSet());
		JVM_METRIC_REGISTRY.registerAll(new GarbageCollectorMetricSet());
		JVM_METRIC_REGISTRY.registerAll(new MemoryUsageGaugeSet());
		JVM_METRIC_REGISTRY.registerAll(new ThreadStatesGaugeSet());
	}

	@Override
	protected MetricRegistry getMetricRegistry() {
		return JVM_METRIC_REGISTRY;
	}

	@Override
	public void contextInitialized(ServletContextEvent event) {
		final ServletContext context = event.getServletContext();
		context.setAttribute(JVMMetricsServlet.JVM_METRICS_REGISTRY, getMetricRegistry());
		context.setAttribute(MetricsServlet.METRIC_FILTER, getMetricFilter());
		if (getDurationUnit() != null) {
			context.setInitParameter(MetricsServlet.DURATION_UNIT, getDurationUnit().toString());
		}
		if (getRateUnit() != null) {
			context.setInitParameter(MetricsServlet.RATE_UNIT, getRateUnit().toString());
		}
		if (getAllowedOrigin() != null) {
			context.setInitParameter(MetricsServlet.ALLOWED_ORIGIN, getAllowedOrigin());
		}
		if (getJsonpCallbackParameter() != null) {
			context.setAttribute(MetricsServlet.CALLBACK_PARAM, getJsonpCallbackParameter());
		}
	}
}
