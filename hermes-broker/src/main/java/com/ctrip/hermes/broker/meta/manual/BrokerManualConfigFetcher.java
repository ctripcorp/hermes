package com.ctrip.hermes.broker.meta.manual;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.meta.manual.ManualConfig;
import com.ctrip.hermes.core.meta.manual.ManualConfigFetcher;
import com.ctrip.hermes.env.ManualConfigProvider;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = ManualConfigFetcher.class, value = BrokerManualConfigFetcher.ID)
public class BrokerManualConfigFetcher implements ManualConfigFetcher {

	public static final String ID = "fromConfigProvider";

	private static final Logger log = LoggerFactory.getLogger(BrokerManualConfigFetcher.class);

	@Inject
	private ManualConfigProvider m_configProvider;

	@Override
	public ManualConfig fetchConfig(ManualConfig oldConfig) {
		String manualConfigStr = m_configProvider.fetchManualConfig();
		if (manualConfigStr != null) {
			try {
				return JSON.parseObject(manualConfigStr, ManualConfig.class);
			} catch (RuntimeException e) {
				log.warn("Fetch manual config failed.");
			}
		}
		return null;
	}

}
