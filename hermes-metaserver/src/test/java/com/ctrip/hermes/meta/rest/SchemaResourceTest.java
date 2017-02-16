package com.ctrip.hermes.meta.rest;
//
//import java.io.File;
//import java.io.IOException;
//import java.util.List;
//import java.util.UUID;
//
//import javax.ws.rs.client.Client;
//import javax.ws.rs.client.ClientBuilder;
//import javax.ws.rs.client.Entity;
//import javax.ws.rs.client.Invocation.Builder;
//import javax.ws.rs.client.WebTarget;
//import javax.ws.rs.core.GenericType;
//import javax.ws.rs.core.MediaType;
//import javax.ws.rs.core.Response;
//import javax.ws.rs.core.Response.Status;
//
//import org.glassfish.jersey.media.multipart.FormDataMultiPart;
//import org.glassfish.jersey.media.multipart.MultiPartFeature;
//import org.glassfish.jersey.media.multipart.file.FileDataBodyPart;
//import org.glassfish.jersey.server.ResourceConfig;
//import org.junit.After;
//import org.junit.Assert;
//import org.junit.Before;
//import org.junit.Test;
//import org.unidal.lookup.ComponentTestCase;
//
//import com.alibaba.fastjson.JSON;
//import com.alibaba.fastjson.JSONObject;
//import com.ctrip.hermes.meta.pojo.SchemaView;
//import com.ctrip.hermes.meta.pojo.TopicView;
//import com.ctrip.hermes.meta.server.MetaRestServer;
//import com.google.common.base.Charsets;
//import com.google.common.io.Files;
//
//public class SchemaResourceTest extends ComponentTestCase {
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
//	public void testBackwardCompatibility() throws IOException {
//		String jsonString = Files.toString(new File("src/test/resources/topic-sample.json"), Charsets.UTF_8);
//		TopicView topicView = JSON.parseObject(jsonString, TopicView.class);
//		topicView.setName(topicView.getName() + "_" + UUID.randomUUID());
//
//		ResourceConfig rc = new ResourceConfig();
//		rc.register(MultiPartFeature.class);
//		Client client = ClientBuilder.newClient(rc);
//		WebTarget webTarget = client.target(StandaloneRestServer.HOST);
//		Builder request = webTarget.path("topics/").request();
//		Response response = request.post(Entity.json(topicView));
//		Assert.assertEquals(Status.CREATED.getStatusCode(), response.getStatus());
//		System.out.println(response.getStatus());
//		TopicView createdTopic = response.readEntity(TopicView.class);
//
//		jsonString = Files.toString(new File("src/test/resources/schema-avro-sample.json"), Charsets.UTF_8);
//		SchemaView schemaView = JSON.parseObject(jsonString, SchemaView.class);
//		schemaView.setCompatibility("BACKWARD");
//
//		FormDataMultiPart form = new FormDataMultiPart();
//		File file = new File("src/test/resources/schema-avro-sample.avsc");
//		form.bodyPart(new FileDataBodyPart("file", file, MediaType.MULTIPART_FORM_DATA_TYPE));
//		form.field("schema", JSON.toJSONString(schemaView));
//		form.field("topicId", String.valueOf(createdTopic.getId()));
//		request = webTarget.path("schemas/").request();
//		response = request.post(Entity.entity(form, MediaType.MULTIPART_FORM_DATA_TYPE));
//		Assert.assertEquals(Status.CREATED.getStatusCode(), response.getStatus());
//		System.out.println(response.getStatus());
//		SchemaView createdView = response.readEntity(SchemaView.class);
//
//		form = new FormDataMultiPart();
//		file = new File("src/test/resources/schema-avro-sample-more.avsc");
//		form.bodyPart(new FileDataBodyPart("file", file, MediaType.MULTIPART_FORM_DATA_TYPE));
//		request = webTarget.path("schemas/" + createdView.getId() + "/compatibility").request();
//		response = request.post(Entity.entity(form, MediaType.MULTIPART_FORM_DATA_TYPE));
//		Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
//		System.out.println(response.getStatus());
//		JSONObject parse = JSON.parseObject(response.readEntity(String.class), JSONObject.class);
//		boolean isCompatible = (boolean) parse.get("is_compatible");
//		Assert.assertEquals(false, isCompatible);
//
//		form = new FormDataMultiPart();
//		file = new File("src/test/resources/schema-avro-sample-less.avsc");
//		form.bodyPart(new FileDataBodyPart("file", file, MediaType.MULTIPART_FORM_DATA_TYPE));
//		request = webTarget.path("schemas/" + createdView.getId() + "/compatibility").request();
//		response = request.post(Entity.entity(form, MediaType.MULTIPART_FORM_DATA_TYPE));
//		Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
//		System.out.println(response.getStatus());
//		parse = JSON.parseObject(response.readEntity(String.class), JSONObject.class);
//		isCompatible = (boolean) parse.get("is_compatible");
//		Assert.assertEquals(true, isCompatible);
//
//		request = webTarget.path("schemas/" + createdView.getId()).request();
//		response = request.delete();
//		Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
//
//		request = webTarget.path("topics/" + createdTopic.getName()).request();
//		response = request.delete();
//		Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
//	}
//
//	@Test
//	public void testCreatTopicAndBadSchema() throws IOException {
//		String jsonString = Files.toString(new File("src/test/resources/topic-sample.json"), Charsets.UTF_8);
//		TopicView topicView = JSON.parseObject(jsonString, TopicView.class);
//		topicView.setName(topicView.getName() + "_" + UUID.randomUUID());
//		topicView.setCodecType("avro");
//
//		ResourceConfig rc = new ResourceConfig();
//		rc.register(MultiPartFeature.class);
//		Client client = ClientBuilder.newClient(rc);
//		WebTarget webTarget = client.target(StandaloneRestServer.HOST);
//		Builder request = webTarget.path("topics/").request();
//		Response response = request.post(Entity.json(topicView));
//		Assert.assertEquals(Status.CREATED.getStatusCode(), response.getStatus());
//		TopicView createdTopic = response.readEntity(TopicView.class);
//
//		jsonString = Files.toString(new File("src/test/resources/schema-avro-sample.json"), Charsets.UTF_8);
//		SchemaView schemaView = JSON.parseObject(jsonString, SchemaView.class);
//
//		request = webTarget.path("schemas/").request();
//		FormDataMultiPart form = new FormDataMultiPart();
//		File file = new File("src/test/resources/schema-avro-sample-bad.avsc");
//		form.bodyPart(new FileDataBodyPart("file", file, MediaType.MULTIPART_FORM_DATA_TYPE));
//		form.field("schema", JSON.toJSONString(schemaView));
//		form.field("topicId", String.valueOf(createdTopic.getId()));
//		response = request.post(Entity.entity(form, MediaType.MULTIPART_FORM_DATA_TYPE));
//		request = webTarget.path("schemas/").request();
//		Assert.assertEquals(Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
//		String result = response.readEntity(String.class);
//		System.out.println(result);
//
//		request = webTarget.path("topics/" + createdTopic.getName()).request();
//		response = request.delete();
//		Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
//	}
//
//	@Test
//	public void testCreatTopicAndSchema() throws IOException {
//		String jsonString = Files.toString(new File("src/test/resources/topic-sample.json"), Charsets.UTF_8);
//		TopicView topicView = JSON.parseObject(jsonString, TopicView.class);
//		topicView.setName(topicView.getName() + "_" + UUID.randomUUID());
//
//		ResourceConfig rc = new ResourceConfig();
//		rc.register(MultiPartFeature.class);
//		Client client = ClientBuilder.newClient(rc);
//		WebTarget webTarget = client.target(StandaloneRestServer.HOST);
//		Builder request = webTarget.path("topics/").request();
//		Response response = request.post(Entity.json(topicView));
//		Assert.assertEquals(Status.CREATED.getStatusCode(), response.getStatus());
//		TopicView createdTopic = response.readEntity(TopicView.class);
//
//		request = webTarget.path("schemas/").request();
//		jsonString = Files.toString(new File("src/test/resources/schema-json-sample.json"), Charsets.UTF_8);
//		SchemaView schemaView = JSON.parseObject(jsonString, SchemaView.class);
//		FormDataMultiPart form = new FormDataMultiPart();
//		form.field("schema", JSON.toJSONString(schemaView));
//		form.field("topicId", String.valueOf(createdTopic.getId()));
//		response = request.post(Entity.entity(form, MediaType.MULTIPART_FORM_DATA_TYPE));
//		request = webTarget.path("schemas/").request();
//		Assert.assertEquals(Status.CREATED.getStatusCode(), response.getStatus());
//		SchemaView createdSchema = response.readEntity(SchemaView.class);
//
//		request = webTarget.path("schemas/" + createdSchema.getId()).request();
//		response = request.delete();
//		Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
//
//		request = webTarget.path("topics/" + createdTopic.getName()).request();
//		response = request.delete();
//		Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
//	}
//
//	@Test
//	public void testDownloadFiles() {
//		ResourceConfig rc = new ResourceConfig();
//		rc.register(MultiPartFeature.class);
//		Client client = ClientBuilder.newClient(rc);
//		WebTarget webTarget = client.target(StandaloneRestServer.HOST);
//
//		Builder request = webTarget.path("schemas").request();
//		Response response = request.get();
//		List<SchemaView> schemas = response.readEntity(new GenericType<List<SchemaView>>() {
//		});
//
//		request = webTarget.path("schemas/" + schemas.get(0).getId() + "/schema").request();
//		response = request.get();
//		if (response.getStatusInfo().getStatusCode() != Status.NOT_FOUND.getStatusCode()) {
//			Assert.assertEquals(Status.OK.getStatusCode(), response.getStatusInfo().getStatusCode());
//			File schemaFile = response.readEntity(File.class);
//			System.out.println(schemaFile.getAbsolutePath());
//			Assert.assertTrue(schemaFile.length() > 0);
//			schemaFile.delete();
//		}
//
//		request = webTarget.path("schemas/" + schemas.get(0).getId() + "/jar").request();
//		response = request.get();
//		if (response.getStatusInfo().getStatusCode() != Status.NOT_FOUND.getStatusCode()) {
//			Assert.assertEquals(Status.OK.getStatusCode(), response.getStatusInfo().getStatusCode());
//			File jarFile = response.readEntity(File.class);
//			System.out.println(jarFile.getAbsolutePath());
//			Assert.assertTrue(jarFile.length() > 0);
//			jarFile.delete();
//		}
//	}
//
//	@Test
//	public void testForwardCompatibility() throws IOException {
//		String jsonString = Files.toString(new File("src/test/resources/topic-sample.json"), Charsets.UTF_8);
//		TopicView topicView = JSON.parseObject(jsonString, TopicView.class);
//		topicView.setName(topicView.getName() + "_" + UUID.randomUUID());
//
//		ResourceConfig rc = new ResourceConfig();
//		rc.register(MultiPartFeature.class);
//		Client client = ClientBuilder.newClient(rc);
//		WebTarget webTarget = client.target(StandaloneRestServer.HOST);
//		Builder request = webTarget.path("topics/").request();
//		Response response = request.post(Entity.json(topicView));
//		Assert.assertEquals(Status.CREATED.getStatusCode(), response.getStatus());
//		System.out.println(response.getStatus());
//		TopicView createdTopic = response.readEntity(TopicView.class);
//
//		jsonString = Files.toString(new File("src/test/resources/schema-avro-sample.json"), Charsets.UTF_8);
//		SchemaView schemaView = JSON.parseObject(jsonString, SchemaView.class);
//		schemaView.setCompatibility("FORWARD");
//
//		FormDataMultiPart form = new FormDataMultiPart();
//		File file = new File("src/test/resources/schema-avro-sample.avsc");
//		form.bodyPart(new FileDataBodyPart("file", file, MediaType.MULTIPART_FORM_DATA_TYPE));
//		form.field("schema", JSON.toJSONString(schemaView));
//		form.field("topicId", String.valueOf(createdTopic.getId()));
//		request = webTarget.path("schemas/").request();
//		response = request.post(Entity.entity(form, MediaType.MULTIPART_FORM_DATA_TYPE));
//		if (response.getStatus() != Status.CREATED.getStatusCode()) {
//			System.out.println(response.getStatusInfo().getReasonPhrase());
//		}
//		Assert.assertEquals(Status.CREATED.getStatusCode(), response.getStatus());
//		System.out.println(response.getStatus());
//		SchemaView createdView = response.readEntity(SchemaView.class);
//
//		form = new FormDataMultiPart();
//		file = new File("src/test/resources/schema-avro-sample-more.avsc");
//		form.bodyPart(new FileDataBodyPart("file", file, MediaType.MULTIPART_FORM_DATA_TYPE));
//		request = webTarget.path("schemas/" + createdView.getId() + "/compatibility").request();
//		response = request.post(Entity.entity(form, MediaType.MULTIPART_FORM_DATA_TYPE));
//		Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
//		System.out.println(response.getStatus());
//		JSONObject parse = JSON.parseObject(response.readEntity(String.class), JSONObject.class);
//		boolean isCompatible = (boolean) parse.get("is_compatible");
//		Assert.assertEquals(true, isCompatible);
//
//		form = new FormDataMultiPart();
//		file = new File("src/test/resources/schema-avro-sample-less.avsc");
//		form.bodyPart(new FileDataBodyPart("file", file, MediaType.MULTIPART_FORM_DATA_TYPE));
//		request = webTarget.path("schemas/" + createdView.getId() + "/compatibility").request();
//		response = request.post(Entity.entity(form, MediaType.MULTIPART_FORM_DATA_TYPE));
//		Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
//		System.out.println(response.getStatus());
//		parse = JSON.parseObject(response.readEntity(String.class), JSONObject.class);
//		isCompatible = (boolean) parse.get("is_compatible");
//		Assert.assertEquals(false, isCompatible);
//
//		request = webTarget.path("schemas/" + createdView.getId()).request();
//		response = request.delete();
//		Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
//
//		request = webTarget.path("topics/" + createdTopic.getName()).request();
//		response = request.delete();
//		Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
//	}
//
//	@Test
//	public void testFullCompatibility() throws IOException {
//		String jsonString = Files.toString(new File("src/test/resources/topic-sample.json"), Charsets.UTF_8);
//		TopicView topicView = JSON.parseObject(jsonString, TopicView.class);
//		topicView.setName(topicView.getName() + "_" + UUID.randomUUID());
//
//		ResourceConfig rc = new ResourceConfig();
//		rc.register(MultiPartFeature.class);
//		Client client = ClientBuilder.newClient(rc);
//		WebTarget webTarget = client.target(StandaloneRestServer.HOST);
//		Builder request = webTarget.path("topics/").request();
//		Response response = request.post(Entity.json(topicView));
//		Assert.assertEquals(Status.CREATED.getStatusCode(), response.getStatus());
//		System.out.println(response.getStatus());
//		TopicView createdTopic = response.readEntity(TopicView.class);
//
//		jsonString = Files.toString(new File("src/test/resources/schema-avro-sample.json"), Charsets.UTF_8);
//		SchemaView schemaView = JSON.parseObject(jsonString, SchemaView.class);
//		schemaView.setCompatibility("FULL");
//
//		FormDataMultiPart form = new FormDataMultiPart();
//		File file = new File("src/test/resources/schema-avro-sample.avsc");
//		form.bodyPart(new FileDataBodyPart("file", file, MediaType.MULTIPART_FORM_DATA_TYPE));
//		form.field("schema", JSON.toJSONString(schemaView));
//		form.field("topicId", String.valueOf(createdTopic.getId()));
//		request = webTarget.path("schemas/").request();
//		response = request.post(Entity.entity(form, MediaType.MULTIPART_FORM_DATA_TYPE));
//		Assert.assertEquals(Status.CREATED.getStatusCode(), response.getStatus());
//		System.out.println(response.getStatus());
//		SchemaView createdView = response.readEntity(SchemaView.class);
//
//		form = new FormDataMultiPart();
//		file = new File("src/test/resources/schema-avro-sample-more.avsc");
//		form.bodyPart(new FileDataBodyPart("file", file, MediaType.MULTIPART_FORM_DATA_TYPE));
//		request = webTarget.path("schemas/" + createdView.getId() + "/compatibility").request();
//		response = request.post(Entity.entity(form, MediaType.MULTIPART_FORM_DATA_TYPE));
//		Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
//		System.out.println(response.getStatus());
//		JSONObject parse = JSON.parseObject(response.readEntity(String.class), JSONObject.class);
//		boolean isCompatible = (boolean) parse.get("is_compatible");
//		Assert.assertEquals(false, isCompatible);
//
//		form = new FormDataMultiPart();
//		file = new File("src/test/resources/schema-avro-sample-less.avsc");
//		form.bodyPart(new FileDataBodyPart("file", file, MediaType.MULTIPART_FORM_DATA_TYPE));
//		request = webTarget.path("schemas/" + createdView.getId() + "/compatibility").request();
//		response = request.post(Entity.entity(form, MediaType.MULTIPART_FORM_DATA_TYPE));
//		Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
//		System.out.println(response.getStatus());
//		parse = JSON.parseObject(response.readEntity(String.class), JSONObject.class);
//		isCompatible = (boolean) parse.get("is_compatible");
//		Assert.assertEquals(false, isCompatible);
//
//		request = webTarget.path("schemas/" + createdView.getId()).request();
//		response = request.delete();
//		Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
//
//		request = webTarget.path("topics/" + createdTopic.getName()).request();
//		response = request.delete();
//		Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
//	}
//
//	@Test
//	public void testListSchemas() {
//		Client client = ClientBuilder.newClient();
//		WebTarget webTarget = client.target(StandaloneRestServer.HOST);
//		Builder request = webTarget.path("schemas").request();
//		Response response = request.get();
//		Assert.assertEquals(Status.OK.getStatusCode(), response.getStatusInfo().getStatusCode());
//		List<SchemaView> schemas = response.readEntity(new GenericType<List<SchemaView>>() {
//		});
//		System.out.println(schemas);
//		Assert.assertTrue(schemas.size() > 0);
//
//		request = webTarget.path("schemas/" + schemas.get(0).getId()).request();
//		SchemaView actual = request.get(SchemaView.class);
//		System.out.println(actual);
//		Assert.assertEquals(schemas.get(0).getId(), actual.getId());
//	}
//
//	@Test
//	public void testPostExistingAvroSchema() throws IOException {
//		String jsonString = Files.toString(new File("src/test/resources/topic-sample.json"), Charsets.UTF_8);
//		TopicView topicView = JSON.parseObject(jsonString, TopicView.class);
//		topicView.setName(topicView.getName() + "_" + UUID.randomUUID());
//
//		ResourceConfig rc = new ResourceConfig();
//		rc.register(MultiPartFeature.class);
//		Client client = ClientBuilder.newClient(rc);
//		WebTarget webTarget = client.target(StandaloneRestServer.HOST);
//		Builder request = webTarget.path("topics/").request();
//		Response response = request.post(Entity.json(topicView));
//		Assert.assertEquals(Status.CREATED.getStatusCode(), response.getStatus());
//		System.out.println(response.getStatus());
//		TopicView createdTopic = response.readEntity(TopicView.class);
//
//		jsonString = Files.toString(new File("src/test/resources/schema-avro-sample.json"), Charsets.UTF_8);
//		SchemaView schemaView = JSON.parseObject(jsonString, SchemaView.class);
//
//		FormDataMultiPart form = new FormDataMultiPart();
//		File file = new File("src/test/resources/schema-avro-sample.avsc");
//		form.bodyPart(new FileDataBodyPart("file", file, MediaType.MULTIPART_FORM_DATA_TYPE));
//		form.field("schema", JSON.toJSONString(schemaView));
//		form.field("topicId", String.valueOf(createdTopic.getId()));
//		request = webTarget.path("schemas/").request();
//		response = request.post(Entity.entity(form, MediaType.MULTIPART_FORM_DATA_TYPE));
//		Assert.assertEquals(Status.CREATED.getStatusCode(), response.getStatus());
//		System.out.println(response.getStatus());
//		SchemaView createdView = response.readEntity(SchemaView.class);
//
//		form = new FormDataMultiPart();
//		file = new File("src/test/resources/schema-avro-sample.avsc");
//		form.bodyPart(new FileDataBodyPart("file", file, MediaType.MULTIPART_FORM_DATA_TYPE));
//		form.field("schema", JSON.toJSONString(schemaView));
//		form.field("topicId", String.valueOf(createdTopic.getId()));
//		response = request.post(Entity.entity(form, MediaType.MULTIPART_FORM_DATA_TYPE));
//		Assert.assertEquals(Status.CREATED.getStatusCode(), response.getStatus());
//		System.out.println(response.getStatus());
//		SchemaView updatedView = response.readEntity(SchemaView.class);
//		Assert.assertEquals(createdView.getVersion().intValue() + 1, updatedView.getVersion().intValue());
//
//		request = webTarget.path("schemas/" + createdView.getId()).request();
//		response = request.delete();
//		Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
//
//		request = webTarget.path("schemas/" + updatedView.getId()).request();
//		response = request.delete();
//		Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
//
//		request = webTarget.path("topics/" + createdTopic.getName()).request();
//		response = request.delete();
//		Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
//	}
//
//	@Test
//	public void testPostNewAvroSchema() throws IOException {
//		String jsonString = Files.toString(new File("src/test/resources/topic-sample.json"), Charsets.UTF_8);
//		TopicView topicView = JSON.parseObject(jsonString, TopicView.class);
//		topicView.setName(topicView.getName() + "_" + UUID.randomUUID());
//
//		ResourceConfig rc = new ResourceConfig();
//		rc.register(MultiPartFeature.class);
//		Client client = ClientBuilder.newClient(rc);
//		WebTarget webTarget = client.target(StandaloneRestServer.HOST);
//		Builder request = webTarget.path("topics/").request();
//		Response response = request.post(Entity.json(topicView));
//		Assert.assertEquals(Status.CREATED.getStatusCode(), response.getStatus());
//		TopicView createdTopic = response.readEntity(TopicView.class);
//
//		jsonString = Files.toString(new File("src/test/resources/schema-avro-sample.json"), Charsets.UTF_8);
//		SchemaView schemaView = JSON.parseObject(jsonString, SchemaView.class);
//
//		FormDataMultiPart form = new FormDataMultiPart();
//		File file = new File("src/test/resources/schema-avro-sample.avsc");
//		form.bodyPart(new FileDataBodyPart("file", file, MediaType.MULTIPART_FORM_DATA_TYPE));
//		form.field("schema", JSON.toJSONString(schemaView));
//		form.field("topicId", String.valueOf(createdTopic.getId()));
//		request = webTarget.path("schemas/").request();
//		response = request.post(Entity.entity(form, MediaType.MULTIPART_FORM_DATA_TYPE));
//		Assert.assertEquals(Status.CREATED.getStatusCode(), response.getStatus());
//		System.out.println(response.getStatus());
//		SchemaView createdView = response.readEntity(SchemaView.class);
//		System.out.println(createdView.getSchemaPreview());
//
//		request = webTarget.path("schemas/" + createdView.getId()).request();
//		response = request.delete();
//		Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
//
//		request = webTarget.path("topics/" + createdTopic.getName()).request();
//		response = request.delete();
//		Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
//	}
//
//	@Test
//	public void testPostNewJsonSchema() throws IOException {
//		String jsonString = Files.toString(new File("src/test/resources/topic-sample.json"), Charsets.UTF_8);
//		TopicView topicView = JSON.parseObject(jsonString, TopicView.class);
//		topicView.setName(topicView.getName() + "_" + UUID.randomUUID());
//
//		ResourceConfig rc = new ResourceConfig();
//		rc.register(MultiPartFeature.class);
//		Client client = ClientBuilder.newClient(rc);
//		WebTarget webTarget = client.target(StandaloneRestServer.HOST);
//		Builder request = webTarget.path("topics/").request();
//		Response response = request.post(Entity.json(topicView));
//		Assert.assertEquals(Status.CREATED.getStatusCode(), response.getStatus());
//		TopicView createdTopic = response.readEntity(TopicView.class);
//
//		jsonString = Files.toString(new File("src/test/resources/schema-json-sample.json"), Charsets.UTF_8);
//		SchemaView schemaView = JSON.parseObject(jsonString, SchemaView.class);
//
//		FormDataMultiPart form = new FormDataMultiPart();
//		File file = new File("src/test/resources/schema-json-sample.json");
//		form.bodyPart(new FileDataBodyPart("file", file, MediaType.MULTIPART_FORM_DATA_TYPE));
//		form.field("schema", JSON.toJSONString(schemaView));
//		form.field("topicId", String.valueOf(createdTopic.getId()));
//		request = webTarget.path("schemas/").request();
//		response = request.post(Entity.entity(form, MediaType.MULTIPART_FORM_DATA_TYPE));
//		Assert.assertEquals(Status.CREATED.getStatusCode(), response.getStatus());
//		System.out.println(response.getStatus());
//		SchemaView createdView = response.readEntity(SchemaView.class);
//
//		request = webTarget.path("schemas/" + createdView.getId()).request();
//		response = request.delete();
//		Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
//
//		request = webTarget.path("topics/" + createdTopic.getName()).request();
//		response = request.delete();
//		Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
//	}
//}
