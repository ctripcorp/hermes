package com.ctrip.hermes.metaserver.rest.resource;

import java.util.List;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaserver.commons.MetaUtils;
import com.ctrip.hermes.metaserver.meta.MetaHolder;
import com.ctrip.hermes.metaserver.rest.commons.RestException;

@Path("/meta/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class MetaResource {

	private static final Logger logger = LoggerFactory.getLogger(MetaResource.class);

	private MetaHolder m_metaHolder = PlexusComponentLocator.lookup(MetaHolder.class);

	@GET
	@Path("complete")
	public Response getCompleteMeta(@QueryParam("version") @DefaultValue("0") long version,
	      @QueryParam("hashCode") @DefaultValue("0") long hashCode) {
		logger.debug("get meta, version {}", version);
		Meta meta = null;
		try {
			meta = m_metaHolder.getMeta();
			if (meta == null) {
				throw new RestException("Meta not found", Status.NOT_FOUND);
			}
			if (!isMetaModified(version, hashCode, meta)) {
				return Response.status(Status.NOT_MODIFIED).build();
			}
		} catch (Exception e) {
			logger.warn("get meta failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.OK).entity(m_metaHolder.getMetaJson(true)).build();
	}

	@GET
	public Response getMeta(@QueryParam("version") @DefaultValue("0") long version,
	      @QueryParam("hashCode") @DefaultValue("0") long hashCode) {
		Meta meta = m_metaHolder.getMeta();
		if (meta == null) {
			throw new RestException("Meta not found", Status.NOT_FOUND);
		}
		if (!isMetaModified(version, hashCode, meta)) {
			return Response.status(Status.NOT_MODIFIED).build();
		}

		return Response.status(Status.OK).entity(m_metaHolder.getMetaJson(false)).build();
	}

	@GET
	@Path("client")
	public Response getCompressedClientMeta(@QueryParam("version") @DefaultValue("0") long version,
	      @QueryParam("hashCode") @DefaultValue("0") long hashCode) throws Exception {
		Meta meta = m_metaHolder.getMeta();
		if (meta == null) {
			throw new RestException("Meta not found", Status.NOT_FOUND);
		}
		if (!isMetaModified(version, hashCode, meta)) {
			return Response.status(Status.NOT_MODIFIED).build();
		}
		
		return Response.status(Status.OK).entity(m_metaHolder.getCompressedClientMeta()).build();
	}

	@POST
	@Path("topics")
	public Response getTopicsMeta(List<String> topics) {
		Meta meta = m_metaHolder.getMeta();
		if (meta == null) {
			throw new RestException("Meta not found", Status.NOT_FOUND);
		}

		if (topics == null || topics.isEmpty()) {
			return Response.status(Status.OK).entity(MetaUtils.filterSensitiveField(meta)).build();
		} else {
			Meta topicsMeta = new Meta();
			topicsMeta.setVersion(meta.getVersion());
			topicsMeta.setId(meta.getId());

			if (meta.getEndpoints() != null && !meta.getEndpoints().isEmpty()) {
				for (Endpoint endpoint : meta.getEndpoints().values()) {
					topicsMeta.addEndpoint(endpoint);
				}

				for (String topic : topics) {
					Topic topicFound = meta.findTopic(topic);
					if (topicFound != null) {
						topicsMeta.addTopic(topicFound);
					}
				}
			}

			return Response.status(Status.OK).entity(topicsMeta).build();
		}
	}

	private boolean isMetaModified(long version, long hashCode, Meta meta) {
		return !(version > 0 && meta.getVersion().equals(version)) || (hashCode > 0 && meta.hashCode() == hashCode);
	}
}
