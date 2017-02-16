package com.ctrip.hermes.metrics;

import java.net.InetSocketAddress;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.servlets.HealthCheckServlet;
import com.codahale.metrics.servlets.MetricsServlet;
import com.codahale.metrics.servlets.PingServlet;
import com.codahale.metrics.servlets.ThreadDumpServlet;

public class HttpMetricsServer {
	private static final Logger logger = LoggerFactory.getLogger(HttpMetricsServer.class);

	private Server server;

	private int port;

	private String bindAddress;

	public HttpMetricsServer(String bindAddress, int port) {
		this.port = port;
		this.bindAddress = bindAddress;

		init();
	}

	private void init() {
		logger.info("Initializing Broker Http Metrics Reporter");

		InetSocketAddress inetSocketAddress = new InetSocketAddress(bindAddress, port);

		server = new Server(inetSocketAddress);

		ServletContextHandler servletContextHandler = new ServletContextHandler();

		servletContextHandler.setContextPath("/");

		servletContextHandler.addEventListener(new MetricsServletContextListener());
		servletContextHandler.addEventListener(new JVMMetricsServletContextListener());
		servletContextHandler.addEventListener(new HealthCheckServletContextListener());

		servletContextHandler.addServlet(new ServletHolder(new HermesServlet()), "/hermes");
		servletContextHandler.addServlet(new ServletHolder(new MetricsServlet()), "/metrics/metrics");
		servletContextHandler.addServlet(new ServletHolder(new ThreadDumpServlet()), "/metrics/threads");
		servletContextHandler.addServlet(new ServletHolder(new HealthCheckServlet()), "/metrics/healthcheck");
		servletContextHandler.addServlet(new ServletHolder(new PingServlet()), "/metrics/ping");

		server.setHandler(servletContextHandler);
		logger.info("Finished initializing Broker Http Metrics Reporter");
	}

	public void start() {
		try {
			logger.info("Starting Broker Http Metrics Reporter");
			server.start();
			logger.info("Started Broker Http Metrics Reporter on: " + bindAddress + ":" + port);
		} catch (Exception e) {
			logger.warn(e.getMessage());
		}
	}

	public void stop() {
		try {
			logger.info("Stopping Broker Http Metrics Reporter");
			server.stop();
			logger.info("Broker Http Metrics Reporter stopped");
		} catch (Exception e) {
			logger.warn(e.getMessage());
		}
	}

	public static void main(String[] args) {
		Counter counter = HermesMetricsRegistry.getMetricRegistry().counter("Sample");
		counter.inc(100);
		HttpMetricsServer server = new HttpMetricsServer("127.0.0.1", 9999);
		server.start();
	}
}
