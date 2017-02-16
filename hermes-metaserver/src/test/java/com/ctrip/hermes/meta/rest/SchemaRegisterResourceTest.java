package com.ctrip.hermes.meta.rest;

import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.WebTarget;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ctrip.hermes.metaserver.StartMetaServer;

public class SchemaRegisterResourceTest {
	StartMetaServer metaServer;

	@Before
	public void init() throws Exception {
		metaServer = new StartMetaServer();
		metaServer.startServer();
	}

	@After
	public void close() throws Exception {
		metaServer.stopServer();
	}

	@Test
	public void testGetSchema() {
		Client client = ClientBuilder.newClient();
		WebTarget webTarget = client.target("http://localhost:1248");
		Builder request = webTarget.path("/schema/register").queryParam("id", 142).request();
		String schema = request.get(String.class);
		System.out.println(schema);
		Assert.assertNotNull(schema);
	}

	@Test
	public void testRegisterSchema() {
		String schemaString = "{\"namespace\": \"com.ctrip.hermes.kafka.avro\", \"type\": \"record\", \"name\": \"AvroVisitEvent\", \"fields\": [ {\"name\": \"ip\", \"type\": \"string\"}, {\"name\": \"url\", \"type\": \"string\"}, {\"name\": \"tz\", \"type\": \"long\", \"java-class\":\"java.util.Date\"} ] }";
		Client client = ClientBuilder.newClient();
		WebTarget webTarget = client.target("http://localhost:1248");
		Builder request = webTarget.path("/schema/register").request();
		Map<String, String> params = new HashMap<String, String>();
		params.put("schema", schemaString);
		params.put("subject", "test-subject-001");
		int id = request.post(Entity.json(params), Integer.class);
		System.out.println(id);
		Assert.assertTrue(id > 0);
	}
}
