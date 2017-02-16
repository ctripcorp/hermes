package com.ctrip.hermes.meta.service;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.avro.Schema.Parser;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.env.ClientEnvironment;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class SchemaRegistryTest {

	public static void main(String[] args) throws IOException, RestClientException {
		ClientEnvironment m_env = PlexusComponentLocator.lookup(ClientEnvironment.class);
		Properties globalConfig = m_env.getGlobalConfig();
		String schemaServerHost = globalConfig.getProperty("schema.server.host");
		String schemaServerPort = globalConfig.getProperty("schema.server.port");
		SchemaRegistryClient avroSchemaRegistry = new CachedSchemaRegistryClient("http://" + schemaServerHost + ":"
		      + schemaServerPort, 1000);
		String schemaName = "AvroVisitEventMore-value";
		String schemaContent = Files.toString(new File("src/test/resources/schema-avro-sample.avsc"), Charsets.UTF_8);
		Parser parser = new Parser();
		org.apache.avro.Schema avroSchema = parser.parse(schemaContent);
		int avroid = avroSchemaRegistry.register(schemaName, avroSchema);
		System.out.println(avroid);

		schemaContent = Files.toString(new File("src/test/resources/schema-avro-sample-more.avsc"), Charsets.UTF_8);
		parser = new Parser();
		avroSchema = parser.parse(schemaContent);
		avroid = avroSchemaRegistry.register(schemaName, avroSchema);
		System.out.println(avroid);

		schemaContent = Files.toString(new File("src/test/resources/schema-avro-sample-less.avsc"), Charsets.UTF_8);
		parser = new Parser();
		avroSchema = parser.parse(schemaContent);
		avroid = avroSchemaRegistry.register(schemaName, avroSchema);
		System.out.println(avroid);

		schemaContent = Files.toString(new File("src/test/resources/schema-avro-sample-bad.avsc"), Charsets.UTF_8);
		parser = new Parser();
		avroSchema = parser.parse(schemaContent);
		avroid = avroSchemaRegistry.register(schemaName, avroSchema);
		System.out.println(avroid);
	}
}
