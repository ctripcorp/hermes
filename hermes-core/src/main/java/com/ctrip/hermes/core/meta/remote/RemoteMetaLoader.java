package com.ctrip.hermes.core.meta.remote;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.helper.Files.IO;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.bo.ClientMeta;
import com.ctrip.hermes.core.config.CoreConfig;
import com.ctrip.hermes.core.meta.internal.MetaLoader;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.env.ClientEnvironment;
import com.ctrip.hermes.meta.entity.Meta;
import com.google.common.base.Charsets;

@Named(type = MetaLoader.class, value = RemoteMetaLoader.ID)
public class RemoteMetaLoader implements MetaLoader {

	private static final Logger log = LoggerFactory.getLogger(RemoteMetaLoader.class);

	public static final String ID = "remote-meta-loader";

	@Inject
	private ClientEnvironment m_clientEnvironment;

	@Inject
	private MetaServerLocator m_metaServerLocator;

	@Inject
	private CoreConfig m_config;

	private AtomicReference<Meta> m_metaCache = new AtomicReference<Meta>(null);

	@Override
	public Meta load() {
		List<String> metaServerList = m_metaServerLocator.getMetaServerList();
		if (metaServerList == null || metaServerList.isEmpty()) {
			throw new RuntimeException("No meta server found.");
		}

		List<String> ipPorts = new ArrayList<String>(metaServerList);

		Collections.shuffle(ipPorts);

		Meta fetchedMeta = null;
		for (String ipPort : ipPorts) {
			if (log.isDebugEnabled()) {
				log.debug("Loading meta from server: {}", ipPort);
			}

			String url = null;
			InputStream is = null;
			try {
				String uri = m_clientEnvironment.getGlobalConfig().getProperty("meta.fetch.remote.uri");
				String defaultClientMetaUri = "/meta/client";
				if (StringUtils.isBlank(uri)) {
					if (log.isDebugEnabled()) {
						log.debug("Use '{}' as default meta fetch uri.", defaultClientMetaUri);
					}
					uri = defaultClientMetaUri;
				}

				url = String.format("http://%s%s", ipPort, uri);
				if (m_metaCache.get() != null) {
					url += "?version=" + m_metaCache.get().getVersion();
				}

				HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();

				conn.setRequestMethod("GET");
				conn.setConnectTimeout(m_config.getMetaServerConnectTimeout());
				conn.setReadTimeout(m_config.getMetaServerReadTimeout());
				conn.connect();

				int statusCode = conn.getResponseCode();

				if (statusCode == 200) {
					is = conn.getInputStream();
					if (defaultClientMetaUri.equals(uri)) {
						byte[] responseContent = IO.INSTANCE.readFrom(is);
						try {
							fetchedMeta = ClientMeta.deserialize(responseContent);
							m_metaCache.set(fetchedMeta);
							return m_metaCache.get();
						} catch (Exception e) {
							log.warn("Parse meta failed, from URL {}, Reason {}", url, e.getMessage());
							log.warn("Got meta: " + responseContent);
						}
					} else {
						String responseContent = IO.INSTANCE.readFrom(is, Charsets.UTF_8.name());
						try {
							fetchedMeta = JSON.parseObject(responseContent, Meta.class);
							m_metaCache.set(fetchedMeta);
							return m_metaCache.get();
						} catch (Exception e) {
							log.warn("Parse meta failed, from URL {}, Reason {}", url, e.getMessage());
							log.warn("Got meta: " + responseContent);
						}
					}
				} else if (statusCode == 304) {
					return m_metaCache.get();
				} else {
					throw new Exception(String.format("Request %s got status code %s.", url, statusCode));
				}

			} catch (Exception e) {
				log.warn("Load meta failed, from URL {}, will retry other meta servers, Reason {}", url, e.getMessage());
				// ignore
			} finally {
				if (is != null) {
					try {
						is.close();
					} catch (Exception e) {
						// ignore it
					}
				}
			}
		}
		throw new RuntimeException(String.format("Failed to load remote meta from %s", ipPorts));
	}

}
