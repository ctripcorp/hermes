package com.ctrip.hermes.core.meta.remote;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.helper.Files.IO;
import org.unidal.helper.Urls;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.config.CoreConfig;
import com.ctrip.hermes.core.meta.manual.ManualConfigService;
import com.ctrip.hermes.core.utils.CollectionUtil;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.env.ClientEnvironment;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Event;
import com.google.common.base.Charsets;

@Named(type = MetaServerLocator.class)
public class DefaultMetaServerLocator implements MetaServerLocator, Initializable {

	private static final Logger log = LoggerFactory.getLogger(DefaultMetaServerLocator.class);

	private static final int DEFAULT_METASERVER_PORT = 80;

	@Inject
	private ClientEnvironment m_clientEnv;

	@Inject
	private CoreConfig m_coreConfig;

	@Inject
	private ManualConfigService m_manualConfigService;

	private AtomicReference<List<String>> m_metaServerList = new AtomicReference<List<String>>(new ArrayList<String>());

	private int m_defaultMetaServerPort = DEFAULT_METASERVER_PORT;

	@Override
	public List<String> getMetaServerList() {
		return m_metaServerList.get();
	}

	private int updateMetaServerList() {
		if (m_manualConfigService.isManualConfigModeOn()) {
			log.debug("Manual config mode is on, will not refresh meta from meta-server");
			return 1;
		}

		int nextUpdateIntevalSec = m_coreConfig.getMetaServerIpFetchInterval();

		int maxTries = 10;
		RuntimeException exception = null;

		for (int retries = 0; retries < maxTries; retries++) {
			try {
				if (CollectionUtil.isNullOrEmpty(m_metaServerList.get()) || retries > 0) {
					String domain = m_clientEnv.getMetaServerDomainName();
					if (retries > 0) {
						log.warn("Can not update meta server list, will retry with domain {}:{}", domain,
						      m_defaultMetaServerPort);
					} else {
						log.info("Update meta server list with domain {}:{}", domain, m_defaultMetaServerPort);
					}
					m_metaServerList.set(new ArrayList<>(Arrays.asList(domain + ":" + m_defaultMetaServerPort)));
				}

				List<String> metaServerList = fetchMetaServerListFromExistingMetaServer();
				if (metaServerList != null && !metaServerList.isEmpty()) {
					m_metaServerList.set(metaServerList);
					return nextUpdateIntevalSec;
				}

			} catch (RuntimeException e) {
				exception = e;
			}

			try {
				TimeUnit.SECONDS.sleep(1);
			} catch (InterruptedException e) {
				// ignore it
			}
		}

		if (exception != null) {
			log.warn("Failed to fetch meta server list for {} times", maxTries);
			throw exception;
		} else {
			return nextUpdateIntevalSec;
		}
	}

	private List<String> fetchMetaServerListFromExistingMetaServer() {
		List<String> metaServerList = new ArrayList<String>(m_metaServerList.get());
		if (log.isDebugEnabled()) {
			log.debug("Start fetching meta server ip from meta servers {}", metaServerList);
		}

		Collections.shuffle(metaServerList);

		for (String ipPort : metaServerList) {
			try {
				List<String> result = doFetch(ipPort);
				if (log.isDebugEnabled()) {
					log.debug("Successfully fetched meta server ip from meta server {}", ipPort);
				}
				return result;
			} catch (Exception e) {
				// ignore it
			}
		}

		throw new RuntimeException("Failed to fetch meta server ip list from any meta server: "
		      + metaServerList.toString());
	}

	private List<String> doFetch(String ipPort) throws IOException {
		String url = String.format("http://%s%s?clientTimeMillis=%s", ipPort, "/metaserver/servers/v2",
		      System.currentTimeMillis());

		InputStream is = null;

		try {
			Map<String, List<String>> headers = new HashMap<String, List<String>>();
			is = Urls.forIO()//
			      .connectTimeout(m_coreConfig.getMetaServerConnectTimeout())//
			      .readTimeout(m_coreConfig.getMetaServerReadTimeout())//
			      .openStream(url, headers);

			String response = IO.INSTANCE.readFrom(is, Charsets.UTF_8.name());

			if (headers.containsKey(CoreConfig.TIME_UNSYNC_HEADER)) {
				List<String> values = headers.get(CoreConfig.TIME_UNSYNC_HEADER);
				String diff = "n/a";
				if (values != null && values.size() > 0) {
					diff = values.get(0);
				}

				Cat.logEvent("Hermes.Client", "TimeUnsync", Event.SUCCESS, "diff=" + diff);
				log.warn("Client time is unsync with hermes server, diff is {}", diff);
			}

			return Arrays.asList(JSON.parseArray(response).toArray(new String[0]));
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

	@Override
	public void initialize() throws InitializationException {
		m_defaultMetaServerPort = Integer.parseInt(m_clientEnv.getGlobalConfig()
		      .getProperty("meta.port", String.valueOf(DEFAULT_METASERVER_PORT)).trim());

		updateMetaServerList();
		Executors.newSingleThreadExecutor(HermesThreadFactory.create("MetaServerIpFetcher", true)).submit(new Runnable() {

			@Override
			public void run() {
				while (!Thread.currentThread().isInterrupted()) {
					int nextUpdateIntervalSec = m_coreConfig.getMetaServerIpFetchInterval();
					try {
						nextUpdateIntervalSec = updateMetaServerList();
					} catch (RuntimeException e) {
						log.warn("Update meta server list failed", e);
					} finally {
						try {
							TimeUnit.SECONDS.sleep(nextUpdateIntervalSec);
						} catch (InterruptedException e) {
							Thread.currentThread().interrupt();
						}
					}
				}
			}

		});
	}
}
