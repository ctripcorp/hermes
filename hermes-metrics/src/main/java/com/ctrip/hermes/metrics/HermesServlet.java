package com.ctrip.hermes.metrics;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.servlets.HealthCheckServlet;
import com.codahale.metrics.servlets.MetricsServlet;
import com.codahale.metrics.servlets.PingServlet;
import com.codahale.metrics.servlets.ThreadDumpServlet;

public class HermesServlet extends HttpServlet {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5115168561274109694L;

	private String jvmUri;

	private String healthcheckUri;

	private String pingUri;

	private String metricsUri;

	private String threadsUri;

	private String globalUri;

	private JVMMetricsServlet jvmServlet;

	private HealthCheckServlet healthCheckServlet;

	private PingServlet pingServlet;

	private ThreadDumpServlet threadDumpServlet;

	private MetricsServlet metricServlet;

	private Map<String, MetricsServlet> metricServletGroupByT;

	private Map<String, MetricsServlet> metricServletGroupByTP;

	private Map<String, MetricsServlet> metricServletGroupByTPG;

	private static final String CONTENT_TYPE = "text/html";

	private ServletConfig config;

	public void init(ServletConfig config) throws ServletException {
		this.config = config;
		if (Boolean.valueOf(config.getInitParameter("show-jvm-metrics"))) {
			JVMMetricsServletContextListener listener = new JVMMetricsServletContextListener();
			this.jvmServlet = new JVMMetricsServlet(listener.getMetricRegistry());
			this.jvmServlet.init(config);

			this.jvmUri = "/jvm";
		}

		healthCheckServlet = new HealthCheckServlet(HermesMetricsRegistry.getHealthCheckRegistry());
		healthCheckServlet.init(config);

		pingServlet = new PingServlet();
		pingServlet.init(config);

		threadDumpServlet = new ThreadDumpServlet();
		threadDumpServlet.init(config);

		metricServlet = new MetricsServlet(HermesMetricsRegistry.getMetricRegistry());
		metricServlet.init(config);

		this.metricServletGroupByT = new HashMap<String, MetricsServlet>();
		this.metricServletGroupByTP = new HashMap<String, MetricsServlet>();
		this.metricServletGroupByTPG = new HashMap<String, MetricsServlet>();

		this.healthcheckUri = "/healthcheck";
		this.pingUri = "/ping";
		this.threadsUri = "/threads";
		this.globalUri = "/global";
		this.metricsUri = "/metrics";
	}

	@Override
	protected void service(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		final String uri = req.getPathInfo();
		if (uri == null) {
			super.service(req, resp);
			return;
		}
		if (jvmUri != null && uri.equals(jvmUri)) {
			jvmServlet.service(req, resp);
		} else if (uri.equals(healthcheckUri)) {
			healthCheckServlet.service(req, resp);
		} else if (uri.equals(globalUri)) {
			metricServlet.service(req, resp);
		} else if (uri.equals(pingUri)) {
			pingServlet.service(req, resp);
		} else if (uri.equals(threadsUri)) {
			threadDumpServlet.service(req, resp);
		} else if (uri.startsWith("/t/")) {
			MetricsServlet metricsServlet = getServletFromUri(uri, req.getParameter("topic"), null, null);
			metricsServlet.service(req, resp);
		} else if (uri.startsWith("/tp/")) {
			MetricsServlet metricsServlet = getServletFromUri(uri, req.getParameter("topic"),
			      req.getParameter("partition"), null);
			metricsServlet.service(req, resp);
		} else if (uri.startsWith("/tpg/")) {
			MetricsServlet metricsServlet = getServletFromUri(uri, req.getParameter("topic"),
			      req.getParameter("partition"), req.getParameter("group"));
			metricsServlet.service(req, resp);
		} else {
			super.service(req, resp);
		}
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		final String path = req.getContextPath() + req.getServletPath();

		resp.setStatus(HttpServletResponse.SC_OK);
		resp.setHeader("Cache-Control", "must-revalidate,no-cache,no-store");
		resp.setContentType(CONTENT_TYPE);
		final PrintWriter writer = resp.getWriter();
		try {
			StringBuilder sb = new StringBuilder();
			sb.append("<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\"")
			      .append("\"http://www.w3.org/TR/html4/loose.dtd\"><html><head>")
			      .append("<title>Hermes Metrics</title></head><body>");
			sb.append("<h3>Default Metrics</h3>");
			sb.append("<ul>");
			sb.append("<li><a href=\"").append(path).append(globalUri).append("?pretty=true\">Global</a></li>");
			sb.append("<li><a href=\"").append(path).append(threadsUri).append("?pretty=true\">Threads Dump</a></li>");
			sb.append("<li><a href=\"").append(path).append(jvmUri).append("?pretty=true\">JVM</a></li>");
			sb.append("<li><a href=\"").append(path).append(pingUri).append("?pretty=true\">Ping</a></li>");
			sb.append("<li><a href=\"").append(path).append(healthcheckUri).append("?pretty=true\">Health Check</a></li>");
			sb.append("</ul>");
			sb.append("<h3>Metrics By Topic</h3>");
			sb.append("<ul>");

			String liFormatter = "<li><a href=\"%s\">%s</a></li>";

			Set<String> tKeys = HermesMetricsRegistry.getMetricRegistiesGroupByT().keySet();
			for (String topic : tKeys) {
				if (topic != null && topic.trim().length() != 0) {
					topic = topic.trim();
					String link = String.format("%s?pretty=true&topic=%s", metricsUri + "/t/", topic);
					sb.append(String.format(liFormatter, link, topic));
				}
			}
			sb.append("</ul>");
			sb.append("<h3>Metrics By Topic-Partition</h3>");
			sb.append("<ul>");
			Set<String> tpKeys = HermesMetricsRegistry.getMetricRegistiesGroupByTP().keySet();
			for (String tp : tpKeys) {
				String[] splits = tp.split("\\|");
				if (splits != null && splits.length == 2 && splits[0] != null && splits[0].trim().length() != 0
				      && splits[1] != null && splits[1].trim().length() != 0) {
					String topic = splits[0].trim();
					String partition = splits[1].trim();
					String link = String.format("%s?pretty=true&topic=%s&partition=%s", metricsUri + "/tp/", topic,
					      partition);
					sb.append(String.format(liFormatter, link, topic + "-" + partition));
				}
			}
			sb.append("</ul>");
			sb.append("<h3>Metrics By Topic-Partition-Group</h3>");
			sb.append("<ul>");
			Set<String> tpgKeys = HermesMetricsRegistry.getMetricRegistiesGroupByTPG().keySet();
			for (String tpg : tpgKeys) {
				String[] splits = tpg.split("\\|");
				if (splits != null && splits.length == 3 && splits[0] != null && splits[0].trim().length() != 0
				      && splits[1] != null && splits[1].trim().length() != 0 && splits[2] != null
				      && splits[2].trim().length() != 0) {
					String topic = splits[0].trim();
					String partition = splits[1].trim();
					String group = splits[2].trim();
					String link = String.format("%s?pretty=true&topic=%s&partition=%s&group=%s", metricsUri + "/tpg/",
					      topic, partition, group);
					sb.append(String.format(liFormatter, link, topic + "-" + partition + "-" + group));
				}
			}
			sb.append("</ul>");
			sb.append("</body></html>");
			writer.println(sb.toString());
		} finally {
			writer.close();
		}
	}

	private MetricsServlet getServletFromUri(String fullUri, String topic, String partition, String group)
	      throws ServletException {
		MetricsServlet metricServlet = null;

		if (fullUri.startsWith("/t/")) {
			if (!metricServletGroupByT.containsKey(topic)) {
				MetricRegistry metricRegistry = HermesMetricsRegistry.getMetricRegistryByT(topic);
				metricServlet = new MetricsServlet(metricRegistry);
				metricServlet.init(config);
				metricServletGroupByT.put(topic, metricServlet);
			} else {
				metricServlet = metricServletGroupByT.get(topic);
			}
		} else if (fullUri.startsWith("/tp/")) {
			String tp = topic + "#" + partition;
			if (!metricServletGroupByTP.containsKey(tp)) {
				MetricRegistry metricRegistry = HermesMetricsRegistry.getMetricRegistryByTP(topic,
				      Integer.valueOf(partition));
				metricServlet = new MetricsServlet(metricRegistry);
				metricServlet.init(config);
				metricServletGroupByTP.put(tp, metricServlet);
			} else {
				metricServlet = metricServletGroupByTP.get(tp);
			}
		} else if (fullUri.startsWith("/tpg/")) {
			String tpg = topic + "#" + partition + "#" + group;
			if (!metricServletGroupByTPG.containsKey(tpg)) {
				MetricRegistry metricRegistry = HermesMetricsRegistry.getMetricRegistryByTPG(topic,
				      Integer.valueOf(partition), group);
				metricServlet = new MetricsServlet(metricRegistry);
				metricServlet.init(config);
				metricServletGroupByTPG.put(tpg, metricServlet);
			} else {
				metricServlet = metricServletGroupByTPG.get(tpg);
			}
		}
		return metricServlet;
	}
}
