package com.ctrip.hermes.meta.rest;

//import java.io.IOException;
//
//import javax.ws.rs.client.Client;
//import javax.ws.rs.client.ClientBuilder;
//import javax.ws.rs.client.Entity;
//import javax.ws.rs.client.Invocation.Builder;
//import javax.ws.rs.client.WebTarget;
//import javax.ws.rs.core.Response;
//import javax.ws.rs.core.Response.Status;
//
//import org.junit.After;
//import org.junit.Assert;
//import org.junit.Before;
//import org.junit.Test;
//import org.unidal.lookup.ComponentTestCase;
//
//import com.alibaba.fastjson.JSON;
//import com.ctrip.hermes.meta.entity.Meta;
//import com.ctrip.hermes.meta.server.MetaRestServer;
//
//public class MetaResourceTest extends ComponentTestCase {
//
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
//	public void testGetMeta() {
//		Client client = ClientBuilder.newClient();
//		WebTarget webTarget = client.target(StandaloneRestServer.HOST);
//		Builder request = webTarget.path("meta/").request();
//		Meta actual = request.get(Meta.class);
//		String json = JSON.toJSONString(actual);
//		System.out.println(json);
//		Assert.assertTrue(actual.getTopics().size() > 0);
//	}
//
//	@Test
//	public void testUpdateMeta() throws IOException {
//		Client client = ClientBuilder.newClient();
//		WebTarget webTarget = client.target(StandaloneRestServer.HOST);
//		Builder request = webTarget.path("meta/").request();
//
//		Meta actual = request.get(Meta.class);
//		String json = JSON.toJSONString(actual);
//		System.out.println(json);
//		Assert.assertTrue(actual.getTopics().size() > 0);
//
//		Response response = request.post(Entity.text(json));
//		if (response.getStatus() == Status.CREATED.getStatusCode()) {
//			System.out.println(response.readEntity(Meta.class));
//		} else {
//			System.out.println(response.getStatus());
//			System.out.println(response.readEntity(String.class));
//		}
//	}
//}
