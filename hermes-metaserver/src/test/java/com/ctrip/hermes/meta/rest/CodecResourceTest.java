package com.ctrip.hermes.meta.rest;

//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.UUID;
//
//import javax.ws.rs.client.Client;
//import javax.ws.rs.client.ClientBuilder;
//import javax.ws.rs.client.Invocation.Builder;
//import javax.ws.rs.client.WebTarget;
//import javax.ws.rs.core.GenericType;
//import javax.ws.rs.core.Response;
//import javax.ws.rs.core.Response.Status;
//
//import org.junit.After;
//import org.junit.Assert;
//import org.junit.Before;
//import org.junit.Test;
//import org.unidal.lookup.ComponentTestCase;
//
//import com.ctrip.hermes.meta.pojo.CodecView;
//import com.ctrip.hermes.meta.server.MetaRestServer;
//
//public class CodecResourceTest extends ComponentTestCase {
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
//	public void testGetCodec() {
//		Client client = ClientBuilder.newClient();
//		WebTarget webTarget = client.target(StandaloneRestServer.HOST);
//		String topic = "kafka.SimpleTopic";
//		Builder request = webTarget.path("codecs/" + topic).request();
//		Response response = request.get();
//		Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
//		CodecView codec = response.readEntity(CodecView.class);
//		System.out.println(codec);
//		List<String> codecs = new ArrayList<>();
//		codecs.add("json");
//		codecs.add("avro");
//		Assert.assertTrue(codecs.contains(codec.getType()));
//
//		request = webTarget.path("codecs/" + UUID.randomUUID()).request();
//		response = request.get();
//		Assert.assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
//	}
//
//	@Test
//	public void testListCodecs() {
//		Client client = ClientBuilder.newClient();
//		WebTarget webTarget = client.target(StandaloneRestServer.HOST);
//		Builder request = webTarget.path("codecs/").request();
//		Response response = request.get();
//		Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
//
//		List<CodecView> readEntity = response.readEntity(new GenericType<List<CodecView>>() {
//		});
//		List<String> codecs = new ArrayList<>();
//		codecs.add("json");
//		codecs.add("avro");
//		for (CodecView codec : readEntity) {
//			System.out.println(codec);
//			Assert.assertTrue(codecs.contains(codec.getType()));
//		}
//	}
//}
