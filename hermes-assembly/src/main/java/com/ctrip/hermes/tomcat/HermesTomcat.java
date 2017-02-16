package com.ctrip.hermes.tomcat;

import java.io.File;

import org.apache.catalina.Context;
import org.apache.catalina.Server;
import org.apache.catalina.connector.Connector;
import org.apache.catalina.core.StandardServer;
import org.apache.catalina.core.StandardService;
import org.apache.catalina.core.StandardThreadExecutor;
import org.apache.catalina.startup.Tomcat;
import org.apache.coyote.http11.Http11NioProtocol;

public class HermesTomcat extends Tomcat {

	private StandardThreadExecutor executor;

	private int maxThreads = 500;

	// max client socket connection count < min(maxQueueSize, maxConnections) 
	private int maxQueueSize = 5000;

	private int maxConnections = 5000;
	
	private int acceptCount = 250;

	private int shutdownPort = 9999;

	private String shutdownString = "shutdown-hermes-1002";

	public HermesTomcat() {
		initializeParameters();

		executor = new StandardThreadExecutor();
		executor.setMaxThreads(maxThreads);
		executor.setMaxQueueSize(maxQueueSize);
		executor.setNamePrefix("hermes-tomcat-exec-");
		executor.setName("hermes-tomcat-executor");
	}

	private void initializeParameters() {
		maxThreads = Integer.parseInt(System.getProperty("maxThreads", Integer.toString(maxThreads)));
		maxQueueSize = Integer.parseInt(System.getProperty("maxQueueSize", Integer.toString(maxQueueSize)));
		acceptCount = Integer.parseInt(System.getProperty("acceptCount", Integer.toString(acceptCount)));
		maxConnections = Integer.parseInt(System.getProperty("maxConnections", Integer.toString(maxConnections)));
		shutdownPort = Integer.parseInt(System.getProperty("shutdownPort", Integer.toString(shutdownPort)));
		shutdownString = System.getProperty("shutdownString", shutdownString);
	}

	@Override
	public Server getServer() {
		if (server != null) {
			return server;
		}

		System.setProperty("catalina.useNaming", "false");

		server = new StandardServer();

		initBaseDir();

		server.setPort(shutdownPort);
		server.setShutdown(shutdownString);

		service = new StandardService();
		service.setName("Tomcat");
		service.addExecutor(executor);

		server.addService(service);
		return server;
	}

	@Override
	public Connector getConnector() {
		getServer();
		if (connector != null) {
			return connector;
		}
		connector = new Connector("HTTP/1.1");
		Http11NioProtocol p = (Http11NioProtocol) connector.getProtocolHandler();
		p.setExecutor(executor);
		connector.setPort(port);
		connector.setProperty("acceptCount", Integer.toString(acceptCount));
		connector.setProperty("maxConnections", Integer.toString(maxConnections));
		service.addConnector(connector);
		return connector;
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("usage: java HermesTomcat webRoot port");
			System.exit(1);
		}

		String webRoot = args[0];
		String port = args[1];

		HermesTomcat tomcat = new HermesTomcat();
		System.out.println(String.format("Starting webapp %s at port %s", webRoot, port));
		System.out.println(String.format("acceptCount %s", tomcat.acceptCount));
		System.out.println(String.format("maxConnections %s", tomcat.maxConnections));
		System.out.println(String.format("maxQueueSize %s", tomcat.maxQueueSize));
		System.out.println(String.format("maxThreads %s", tomcat.maxThreads));
		System.out.println(String.format("shutdownPort %s", tomcat.shutdownPort));
		System.out.println(String.format("shutdownString %s", tomcat.shutdownString));

		// Define a folder to hold web application contents.
		File webappDirLocation = new File(webRoot);

		// Bind the port to Tomcat server
		tomcat.setPort(Integer.valueOf(port));

		// Define a web application context.
		Context context = tomcat.addWebapp("", webappDirLocation.getAbsolutePath());

		// Define and bind web.xml file location.
		File configFile = new File(webappDirLocation, "/WEB-INF/web.xml");
		context.setConfigFile(configFile.toURI().toURL());

		tomcat.start();
		tomcat.getServer().await();
		
		tomcat.stop();
	}
	
}
