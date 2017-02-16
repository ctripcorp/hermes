package com.ctrip.hermes.core.meta.manual;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.ctrip.hermes.core.transport.command.v6.FetchManualConfigCommandV6;
import com.ctrip.hermes.core.transport.endpoint.EndpointClient;
import com.ctrip.hermes.core.transport.monitor.FetchManualConfigResultMonitor;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.env.ManualConfigProvider;
import com.ctrip.hermes.meta.entity.Endpoint;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = ManualConfigFetcher.class, value = DefaultManualConfigFetcher.ID)
public class DefaultManualConfigFetcher implements ManualConfigFetcher {

	public static final String ID = "fromBroker";

	private static final Logger log = LoggerFactory.getLogger(DefaultManualConfigFetcher.class);

	@Inject
	private ManualConfigProvider m_configProvider;

	@Inject
	private FetchManualConfigResultMonitor m_configResultMonitor;

	private String m_brokersStr;

	private List<Endpoint> m_brokerEndpoints;

	@Override
	public ManualConfig fetchConfig(ManualConfig oldConfig) {
		String brokersFromConfigProvider = m_configProvider.getBrokers();
		if (brokersFromConfigProvider != null && !StringUtils.equals(m_brokersStr, brokersFromConfigProvider)) {
			m_brokerEndpoints = convertToEndpoints(brokersFromConfigProvider);
			m_brokersStr = brokersFromConfigProvider;
		}

		if (m_brokerEndpoints != null && !m_brokerEndpoints.isEmpty()) {

			Collections.shuffle(m_brokerEndpoints);
			EndpointClient endpointClient = PlexusComponentLocator.lookup(EndpointClient.class);

			for (Endpoint endpoint : m_brokerEndpoints) {
				try {
					FetchManualConfigCommandV6 cmd = new FetchManualConfigCommandV6();
					cmd.setVersion(oldConfig == null ? FetchManualConfigCommandV6.UNSET_VERSION : oldConfig.getVersion());
					Future<ManualConfig> future = m_configResultMonitor.monitor(cmd);
					for (int i = 0; i < 3; i++) {
						if (endpointClient.writeCommand(endpoint, cmd)) {
							try {
								ManualConfig config = future.get(1, TimeUnit.SECONDS);
								if (config != null) {
									log.info("Fetched manual config from broker {}:{}", endpoint.getHost(), endpoint.getPort());
								}
								return config;
							} catch (TimeoutException e) {
								// ignore
							}
						} else {
							TimeUnit.MILLISECONDS.sleep(100);
						}
					}
				} catch (Exception e) {
					log.warn("Fetch manual config from broker {}:{} failed", endpoint.getHost(), endpoint.getPort());
				}
			}
		} else {
			log.info("Can't fetch manual config, since no brokers found.");
		}

		return null;
	}

	private List<Endpoint> convertToEndpoints(String brokers) {
		try {
			Map<String, String> idEndpointMap = JSON.parseObject(brokers, new TypeReference<Map<String, String>>() {
			});
			if (idEndpointMap != null && !idEndpointMap.isEmpty()) {
				List<Endpoint> endpoints = new ArrayList<>();
				for (Map.Entry<String, String> idEndpoint : idEndpointMap.entrySet()) {
					String[] splits = idEndpoint.getValue().split(":");
					String ip = splits[0].trim();
					int port = Integer.parseInt(splits[1].trim());
					Endpoint endpoint = new Endpoint(idEndpoint.getKey());
					endpoint.setHost(ip);
					endpoint.setPort(port);
					endpoint.setType(Endpoint.BROKER);
					endpoints.add(endpoint);
				}
				return endpoints;
			}
		} catch (Exception e) {
			log.warn("Parse brokers string failed.", e);
		}

		return null;
	}
}
