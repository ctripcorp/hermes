package com.ctrip.hermes.metaserver.rest.resource;

import io.confluent.kafka.schemaregistry.client.rest.RestService;

import java.util.Map;

import javax.inject.Singleton;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Request;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.meta.entity.Codec;
import com.ctrip.hermes.metaserver.meta.MetaHolder;
import com.ctrip.hermes.metaserver.rest.commons.RestException;

@Path("/schema/register")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class SchemaRegisterResource {
	private static final Logger log = LoggerFactory.getLogger(SchemaRegisterResource.class);

	private MetaHolder m_metaHolder = PlexusComponentLocator.lookup(MetaHolder.class);

	@GET
	@Path("get")
	public Response getLatestSchemaBySubject(@QueryParam("subject") String subject) {
		if (StringUtils.isBlank(subject)) {
			throw new RestException("Subject is required.", Status.BAD_REQUEST);
		}
		Codec codec = m_metaHolder.getMeta().getCodecs().get("avro");
		if (codec != null) {
			try {
				String url = codec.findProperty("schema.registry.url").getValue();
				if (StringUtils.isBlank(url)) {
					throw new RestException("Can not find schema registry url in codec.", Status.INTERNAL_SERVER_ERROR);
				}
				url = String.format("%s/subjects/%s/versions/latest", url, subject);
				HttpResponse response = Request.Get(url).execute().returnResponse();
				String json = EntityUtils.toString(response.getEntity());
				return Response.status(response.getStatusLine().getStatusCode()).entity(json).build();
			} catch (Exception e) {
				throw new RestException("Register schema failed.", e);
			}
		}
		return Response.status(Status.NOT_FOUND).entity("No avro codec was found!").build();
	}

	@POST
	public Response registerSchema(String content) {
		@SuppressWarnings("unchecked")
		Map<String, String> m = JSON.parseObject(content, Map.class);
		if (StringUtils.isBlank(m.get("schema")) || StringUtils.isBlank(m.get("subject"))) {
			throw new RestException("Schema and subject is required.", Status.BAD_REQUEST);
		}
		Codec codec = m_metaHolder.getMeta().getCodecs().get("avro");
		if (codec != null) {
			try {
				RestService restService = new RestService(codec.findProperty("schema.registry.url").getValue());
				int id = restService.registerSchema(m.get("schema"), m.get("subject"));
				return Response.status(Status.OK).entity(id).build();
			} catch (Exception e) {
				throw new RestException("Register schema failed.", e);
			}
		}
		log.warn("Register schema failed. Codec avro not found.");
		return Response.status(Status.NOT_FOUND).entity("No avro codec was found!").build();
	}

	@GET
	public Response getSchemaById(@QueryParam("id") @DefaultValue("-1") int id) {
		Codec codec = m_metaHolder.getMeta().getCodecs().get("avro");
		if (codec != null) {
			try {
				RestService restService = new RestService(codec.findProperty("schema.registry.url").getValue());
				return Response.status(Status.OK).entity(restService.getId(id).getSchemaString()).build();
			} catch (Exception e) {
				throw new RestException("Get schema string failed.", e);
			}
		}
		log.warn("Get schema string failed. Codec avro not found.");
		return Response.status(Status.NOT_FOUND).entity("No avro codec was found!").build();
	}
}
