package com.ctrip.hermes.metaserver.fulltest;

import java.io.File;
import java.lang.reflect.Field;
import java.net.URLClassLoader;
import java.util.EventListener;

import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.jetty.webapp.WebAppClassLoader;
import org.mortbay.jetty.webapp.WebAppContext;
import org.mortbay.servlet.GzipFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.ContainerHolder;
import org.unidal.test.jetty.ResourceFallbackWebAppContext;
import org.unidal.test.jetty.WebModuleResource;
import org.unidal.test.jetty.WebModuleServlet;

public class HermesClassLoaderMetaServer extends ContainerHolder {
	private static final Logger log = LoggerFactory.getLogger(HermesClassLoaderMetaServer.class);

	int port;

	private Server m_server;
	private WebModuleResource m_resource;

	public HermesClassLoaderMetaServer(int port) {
		this.port = port;
	}

	public void startServer() throws Exception {
		Server server = new Server(port);
		WebAppContext context = new ResourceFallbackWebAppContext();

		configure(context);
		ClassLoader parent = new HermesClassLoader(((URLClassLoader) this.getClass().getClassLoader()).getURLs(),
				  "com.ctrip.", "com.dianping.", "org.unidal.");

		context.setClassLoader(new WebAppClassLoader(parent, context));
		context.addServlet(new ServletHolder(new WebModuleServlet(m_resource)), "/");


		// find LoadMocksListener with HermesClassLoader
		Class clazz = parent.loadClass("com.ctrip.hermes.metaserver.fulltest.LoadMocksListener");
		Object obj = clazz.newInstance();
		Field field = clazz.getDeclaredField("port");
		field.setAccessible(true);
		field.set(obj, port);
		context.addEventListener((EventListener) obj);

		server.addHandler(context);
		try {
			server.start();
		} catch (java.net.BindException e) {
			log.warn("CustomClassLoaderMetaServer[localhost:{}] start fail, BindException.", port);
		}

		postConfigure(context);

		m_server = server;
	}

	public void stopServer() throws Exception {
		m_server.stop();
	}

	@SuppressWarnings("unchecked")
	protected void configure(WebAppContext context) throws Exception {
		File warRoot = getWarRoot();

		m_resource = new WebModuleResource(warRoot);
		context.getInitParams().put("org.mortbay.jetty.servlet.Default.dirAllowed", "false");
		context.setContextPath(getContextPath());
		context.setDescriptor(new File(warRoot, "WEB-INF/web.xml").getPath());
		context.setBaseResource(m_resource);
	}

	protected File getWarRoot() {
		File file = new File("src/main/webapp");
		if (! file.exists()) {
			file = new File("hermes-metaserver/src/main/webapp");
		}
		return file;
	}

	protected void postConfigure(WebAppContext context) {
		context.addFilter(GzipFilter.class, "/*", Handler.ALL);
	}

	protected String getContextPath() {
		return "/";
	}
}
