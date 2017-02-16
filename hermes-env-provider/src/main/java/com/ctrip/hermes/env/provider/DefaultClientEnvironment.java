package com.ctrip.hermes.env.provider;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.env.ClientEnvironment;

@Named(type = ClientEnvironment.class)
public class DefaultClientEnvironment extends ContainerHolder implements ClientEnvironment, Initializable {
	private final static String PRODUCER_DEFAULT_FILE = "/hermes-producer.properties";

	private final static String PRODUCER_PATTERN = "/hermes-producer-%s.properties";

	private final static String CONSUMER_DEFAULT_FILE = "/hermes-consumer.properties";

	private final static String CONSUMER_PATTERN = "/hermes-consumer-%s.properties";

	private final static String GLOBAL_DEFAULT_FILE = "/hermes.properties";

	private ConcurrentMap<String, Properties> m_producerConfigCache = new ConcurrentHashMap<String, Properties>();

	private ConcurrentMap<String, Properties> m_consumerConfigCache = new ConcurrentHashMap<String, Properties>();

	private Properties m_producerDefault;

	private Properties m_consumerDefault;

	private Properties m_globalDefault;

	private static final Logger logger = LoggerFactory.getLogger(DefaultClientEnvironment.class);

	@Inject
	private EnvProvider m_envProvider;

	@Override
	public String getMetaServerDomainName() {
		return m_envProvider.getMetaServerDomainName();
	}

	@Override
	public Properties getProducerConfig(String topic) throws IOException {
		Properties properties = m_producerConfigCache.get(topic);
		if (properties == null) {
			synchronized (m_producerConfigCache) {
				properties = m_producerConfigCache.get(topic);
				if (properties == null) {
					m_producerConfigCache.put(topic,
					      readConfigFile(String.format(PRODUCER_PATTERN, topic), m_producerDefault));
					properties = m_producerConfigCache.get(topic);
				}
			}
		}

		return properties;
	}

	@Override
	public Properties getConsumerConfig(String topic) throws IOException {
		Properties properties = m_consumerConfigCache.get(topic);
		if (properties == null) {
			synchronized (m_consumerConfigCache) {
				properties = m_consumerConfigCache.get(topic);
				if (properties == null) {
					m_consumerConfigCache.put(topic,
					      readConfigFile(String.format(CONSUMER_PATTERN, topic), m_consumerDefault));
					properties = m_consumerConfigCache.get(topic);
				}
			}
		}

		return properties;
	}

	@Override
	public Properties getGlobalConfig() {
		return m_globalDefault;
	}

	private Properties readConfigFile(String configPath) throws IOException {
		return readConfigFile(configPath, null);
	}

	@SuppressWarnings("unchecked")
	private Properties readConfigFile(String configPath, Properties defaults) throws IOException {
		InputStream in = this.getClass().getResourceAsStream(configPath);
		logger.info("Reading config from resource {}", configPath);
		if (in == null) {
			// load outside resource under current user path
			Path path = new File(System.getProperty("user.dir") + configPath).toPath();
			if (Files.isReadable(path)) {
				in = new FileInputStream(path.toFile());
				logger.info("Reading config from file {} ", path);
			}
		}
		Properties props = new Properties();
		if (defaults != null) {
			props.putAll(defaults);
		}

		if (in != null) {
			props.load(in);
		}

		StringBuilder sb = new StringBuilder();
		for (Enumeration<String> e = (Enumeration<String>) props.propertyNames(); e.hasMoreElements();) {
			String key = e.nextElement();
			String val = (String) props.getProperty(key);
			sb.append(key).append('=').append(val).append('\n');
		}
		logger.info("Reading properties: \n" + sb.toString());
		return props;
	}

	@Override
	public void initialize() throws InitializationException {
		try {
			m_producerDefault = readConfigFile(PRODUCER_DEFAULT_FILE);
			m_consumerDefault = readConfigFile(CONSUMER_DEFAULT_FILE);
			m_globalDefault = readConfigFile(GLOBAL_DEFAULT_FILE);
		} catch (IOException e) {
			throw new InitializationException("Error read producer default config file", e);
		}

		m_envProvider.initialize(m_globalDefault);
	}

	@Override
	public String getEnv() {
		return m_envProvider.getEnv();
	}

	@Override
	public String getIdc() {
		return m_envProvider.getIdc();
	}

}
