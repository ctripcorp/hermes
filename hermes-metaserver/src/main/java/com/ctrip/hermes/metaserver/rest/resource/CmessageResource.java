package com.ctrip.hermes.metaserver.rest.resource;

import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.inject.Singleton;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.metaservice.zk.ZKClient;
import com.ctrip.hermes.metaservice.zk.ZKPathUtils;
import com.ctrip.hermes.metaservice.zk.ZKSerializeUtils;

@Path("/cmessage/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class CmessageResource {
	private AtomicReference<String> m_info = new AtomicReference<String>("");

	private AtomicReference<String> m_cmsgConfig = new AtomicReference<String>("");

	private AtomicLong m_cmsgConfigVersion = new AtomicLong(0);

	private ZKClient m_zkClient = PlexusComponentLocator.lookup(ZKClient.class);

	private NodeCache m_exchangeInfoCache;

	private NodeCache m_cmsgConfigCache;

	public CmessageResource() {
		m_exchangeInfoCache = new NodeCache(m_zkClient.get(), ZKPathUtils.getCmessageExchangePath());
		m_exchangeInfoCache.getListenable().addListener(new NodeCacheListener() {

			@Override
			public void nodeChanged() throws Exception {
				updateExchangeInfo();
			}

		}, Executors.newSingleThreadExecutor());
		try {
			m_exchangeInfoCache.start(true);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		m_cmsgConfigCache = new NodeCache(m_zkClient.get(), ZKPathUtils.getCmessageConfigPath());
		m_cmsgConfigCache.getListenable().addListener(new NodeCacheListener() {

			@Override
			public void nodeChanged() throws Exception {
				updateCmsgConfig();
			}

		}, Executors.newSingleThreadExecutor());
		try {
			m_cmsgConfigCache.start(true);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		updateExchangeInfo();
		updateCmsgConfig();
	}

	private void updateExchangeInfo() {
		ChildData childData = m_exchangeInfoCache.getCurrentData();
		if (childData != null) {
			m_info.set(ZKSerializeUtils.deserialize(childData.getData(), String.class));
		}
	}

	private void updateCmsgConfig() {
		ChildData childData = m_cmsgConfigCache.getCurrentData();
		if (childData != null) {
			m_cmsgConfig.set(ZKSerializeUtils.deserialize(childData.getData(), String.class));
		}
	}

	@GET
	@Path("exchange")
	public Response getExchangeList() {
		String str = m_info.get();
		if (str == null || str.length() == 0) {
			str = "[]";
		}
		return Response.status(Status.OK).entity(str).build();
	}

	@GET
	@Path("config")
	public Response getConfig(@QueryParam("version") @DefaultValue("0") long version) {
		if (version > 0 && version == m_cmsgConfigVersion.get()) {
			return Response.status(Status.NOT_MODIFIED).build();
		} else {
			return Response.status(Status.OK).entity(m_cmsgConfig.get()).build();
		}
	}
}
