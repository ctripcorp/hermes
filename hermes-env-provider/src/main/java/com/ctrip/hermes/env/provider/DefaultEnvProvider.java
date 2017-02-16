package com.ctrip.hermes.env.provider;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Named;

@Named(type = EnvProvider.class)
public class DefaultEnvProvider implements EnvProvider {

	private static final Logger log = LoggerFactory.getLogger(DefaultEnvProvider.class);

	private String m_env;

	private String m_metaServerDomainName;

	private String m_idc;

	@Override
	public String getEnv() {
		return m_env;
	}

	public void initialize(Properties config) {
		m_env = config.getProperty("env");
		m_metaServerDomainName = config.getProperty("meta.domain");
		m_idc = config.getProperty("idc");

		log.info("Hermes env: {}, meta.domain: {}, idc: {}", m_env, m_metaServerDomainName, m_idc);
	}

	@Override
	public String getMetaServerDomainName() {
		return m_metaServerDomainName;
	}

	@Override
	public String getIdc() {
		return m_idc;
	}

}
