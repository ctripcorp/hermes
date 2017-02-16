package com.ctrip.hermes.metrics;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.json.MetricsModule;
import com.codahale.metrics.servlets.MetricsServlet;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.util.JSONPObject;

public class JVMMetricsServlet extends MetricsServlet {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7453478195198532356L;

	public static final String JVM_METRICS_REGISTRY = JVMMetricsServlet.class.getCanonicalName() + ".jvmRegistry";;

	private static final String CONTENT_TYPE = "application/json";

	private String allowedOrigin;

	private String jsonpParamName;

	private transient MetricRegistry registry;

	private transient ObjectMapper mapper;

	public JVMMetricsServlet(MetricRegistry registry) {
		this.registry = registry;
	}

	@Override
	public void init(ServletConfig config) throws ServletException {
		final ServletContext context = config.getServletContext();
		if (null == registry) {
			final Object registryAttr = context.getAttribute(JVM_METRICS_REGISTRY);
			if (registryAttr instanceof MetricRegistry) {
				this.registry = (MetricRegistry) registryAttr;
			} else {
				throw new ServletException("Couldn't find a JVMMetricRegistry instance.");
			}
		}

		final TimeUnit rateUnit = parseTimeUnit(context.getInitParameter(RATE_UNIT), TimeUnit.SECONDS);
		final TimeUnit durationUnit = parseTimeUnit(context.getInitParameter(DURATION_UNIT), TimeUnit.SECONDS);
		final boolean showSamples = Boolean.parseBoolean(context.getInitParameter(SHOW_SAMPLES));
		MetricFilter filter = (MetricFilter) context.getAttribute(METRIC_FILTER);
		if (filter == null) {
			filter = MetricFilter.ALL;
		}
		this.mapper = new ObjectMapper().registerModule(new MetricsModule(rateUnit, durationUnit, showSamples, filter));

		this.allowedOrigin = context.getInitParameter(ALLOWED_ORIGIN);
		this.jsonpParamName = context.getInitParameter(CALLBACK_PARAM);
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		resp.setContentType(CONTENT_TYPE);
		if (allowedOrigin != null) {
			resp.setHeader("Access-Control-Allow-Origin", allowedOrigin);
		}
		resp.setHeader("Cache-Control", "must-revalidate,no-cache,no-store");
		resp.setStatus(HttpServletResponse.SC_OK);

		final OutputStream output = resp.getOutputStream();
		try {
			if (jsonpParamName != null && req.getParameter(jsonpParamName) != null) {
				getWriter(req).writeValue(output, new JSONPObject(req.getParameter(jsonpParamName), registry));
			} else {
				getWriter(req).writeValue(output, registry);
			}
		} finally {
			output.close();
		}
	}

	private ObjectWriter getWriter(HttpServletRequest request) {
		final boolean prettyPrint = Boolean.parseBoolean(request.getParameter("pretty"));
		if (prettyPrint) {
			return mapper.writerWithDefaultPrettyPrinter();
		}
		return mapper.writer();
	}

	private TimeUnit parseTimeUnit(String value, TimeUnit defaultValue) {
		try {
			return TimeUnit.valueOf(String.valueOf(value).toUpperCase(Locale.US));
		} catch (IllegalArgumentException e) {
			return defaultValue;
		}
	}
}
