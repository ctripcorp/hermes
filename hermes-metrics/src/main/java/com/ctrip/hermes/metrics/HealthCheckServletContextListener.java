package com.ctrip.hermes.metrics;

import com.codahale.metrics.health.HealthCheckRegistry;
import com.codahale.metrics.servlets.HealthCheckServlet;

public class HealthCheckServletContextListener extends HealthCheckServlet.ContextListener {

	@Override
	protected HealthCheckRegistry getHealthCheckRegistry() {
		return HermesMetricsRegistry.getHealthCheckRegistry();
	}

}
