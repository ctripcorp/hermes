package com.ctrip.hermes.meta.rest;

//import java.io.IOException;
//
//import javax.ws.rs.client.Client;
//import javax.ws.rs.client.ClientBuilder;
//import javax.ws.rs.client.Invocation.Builder;
//import javax.ws.rs.client.WebTarget;
//import javax.ws.rs.core.Response;
//
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Test;
//import org.unidal.lookup.ComponentTestCase;
//
//import com.ctrip.hermes.meta.server.MetaRestServer;
//
//public class MetaServerResourceTest extends ComponentTestCase {
//	private MetaRestServer server;
//
//	@Before
//	public void startServer() throws IOException {
//		server = lookup(MetaRestServer.class);
//		server.start();
//	}
//
//	@After
//	public void stopServer() {
//		server.stop();
//	}
//
//	@Test
//	public void testGetServers() {
//		Client client = ClientBuilder.newClient();
//		WebTarget webTarget = client.target(StandaloneRestServer.HOST);
//		Builder request = webTarget.path("metaserver/servers").request();
//		Response response = request.get();
//		System.out.println(response.getStatus());
//		System.out.println(response.readEntity(String.class));
//	}
//	
//	@Test
//	public void testGetKafkaBrokers() {
//		Client client = ClientBuilder.newClient();
//		WebTarget webTarget = client.target(StandaloneRestServer.HOST);
//		Builder request = webTarget.path("metaserver/kafka/brokers").request();
//		Response response = request.get();
//		System.out.println(response.getStatus());
//		System.out.println(response.readEntity(String.class));
//	}
//	
//	@Test
//	public void testGetZookeeper() {
//		Client client = ClientBuilder.newClient();
//		WebTarget webTarget = client.target(StandaloneRestServer.HOST);
//		Builder request = webTarget.path("metaserver/kafka/zookeeper").request();
//		Response response = request.get();
//		System.out.println(response.getStatus());
//		System.out.println(response.readEntity(String.class));
//	}
// }
