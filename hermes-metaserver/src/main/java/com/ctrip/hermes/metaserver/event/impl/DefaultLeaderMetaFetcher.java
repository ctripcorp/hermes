package com.ctrip.hermes.metaserver.event.impl;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.fluent.Request;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.net.Networks;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.metaserver.config.MetaServerConfig;
import com.ctrip.hermes.metaserver.meta.MetaHolder;
import com.ctrip.hermes.metaserver.meta.MetaInfo;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = LeaderMetaFetcher.class)
public class DefaultLeaderMetaFetcher implements LeaderMetaFetcher {

	private static final Logger log = LoggerFactory.getLogger(DefaultLeaderMetaFetcher.class);

	@Inject
	private MetaServerConfig m_config;

	@Inject
	private MetaHolder m_metaHolder;

	@Override
	public Meta fetchMetaInfo(MetaInfo metaInfo) {
		if (metaInfo == null) {
			return null;
		}

		long start = System.currentTimeMillis();
		try {
			if (Networks.forIp().getLocalHostAddress().equals(metaInfo.getHost())
			      && m_config.getMetaServerPort() == metaInfo.getPort()) {
				return null;
			}

			String url = String.format("http://%s:%s/meta/complete", metaInfo.getHost(), metaInfo.getPort());
			Meta meta = m_metaHolder.getMeta();

			if (meta != null) {
				url += "?version=" + meta.getVersion();
			}

			HttpResponse response = Request.Get(url)//
			      .connectTimeout(m_config.getFetcheMetaFromLeaderConnectTimeout())//
			      .socketTimeout(m_config.getFetcheMetaFromLeaderReadTimeout())//
			      .execute()//
			      .returnResponse();

			if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
				String responseContent = EntityUtils.toString(response.getEntity());
				return JSON.parseObject(responseContent, Meta.class);
			} else if (response.getStatusLine().getStatusCode() == HttpStatus.SC_NOT_MODIFIED) {
				return meta;
			}

		} catch (Exception e) {
			log.error("Failed to fetch meta from leader({}:{})", metaInfo.getHost(), metaInfo.getPort(), e);
		} finally {
			log.info("Fetch leader info cost {}ms", (System.currentTimeMillis() - start));
		}
		return null;
	}

}
