package com.ctrip.hermes.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.servlets.MetricsServlet;

public class HermesMetricsServletContextListener extends MetricsServlet.ContextListener {

	@Override
	protected MetricRegistry getMetricRegistry() {
		return HermesMetricsRegistry.getMetricRegistry();
	}

}
