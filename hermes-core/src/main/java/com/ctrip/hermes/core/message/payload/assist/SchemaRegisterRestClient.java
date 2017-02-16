package com.ctrip.hermes.core.message.payload.assist;

import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;
import java.util.Map;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.meta.internal.MetaManager;

@Named(type = SchemaRegisterRestClient.class)
public class SchemaRegisterRestClient extends RestService {

	@Inject
	private MetaManager m_metaManager;

	public SchemaRegisterRestClient() {
		super("http://127.0.0.1:8081");// faked, useless
	}

	@Override
	public int registerSchema(Map<String, String> requestProperties, RegisterSchemaRequest registerSchemaRequest,
	      String subject) throws IOException, RestClientException {
		return m_metaManager.getMetaProxy().registerSchema(registerSchemaRequest.getSchema(), subject);
	}

	@Override
	public SchemaString getId(Map<String, String> requestProperties, int id) throws IOException, RestClientException {
		return new SchemaString(m_metaManager.getMetaProxy().getSchemaString(id));
	}
}
