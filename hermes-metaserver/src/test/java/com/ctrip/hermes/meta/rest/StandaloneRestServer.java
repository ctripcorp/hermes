package com.ctrip.hermes.meta.rest;
//
//import java.io.IOException;
//import java.util.Properties;
//
//import com.ctrip.hermes.core.env.ClientEnvironment;
//import com.ctrip.hermes.core.utils.PlexusComponentLocator;
//import com.ctrip.hermes.meta.server.MetaRestServer;
//
//public class StandaloneRestServer {
//
//	public static String HOST = null;
//
//	static {
//		Properties load;
//		load = PlexusComponentLocator.lookup(ClientEnvironment.class).getGlobalConfig();
//		String host = load.getProperty("meta.host");
//		String port = load.getProperty("meta.port");
//		HOST = "http://" + host + ":" + port + "/";
//
//	}
//
//	public static void main(String[] args) throws InterruptedException, IOException {
//		MetaRestServer server = PlexusComponentLocator.lookup(MetaRestServer.class);
//		server.start();
//		Thread.currentThread().join();
//	}
//}
