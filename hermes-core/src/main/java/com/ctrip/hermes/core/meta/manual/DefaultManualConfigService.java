package com.ctrip.hermes.core.meta.manual;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.meta.internal.MetaProxy;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.env.ManualConfigProvider;
import com.ctrip.hermes.meta.entity.Meta;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = ManualConfigService.class)
public class DefaultManualConfigService extends ContainerHolder implements ManualConfigService, Initializable {

	private static final Logger log = LoggerFactory.getLogger(DefaultManualConfigService.class);

	@Inject
	private ManualConfigProvider m_configProvider;

	private ManualConfigFetcher m_configFetcher;

	private AtomicReference<ManualConfig> m_config = new AtomicReference<>();

	private ManualConfiguredMetaProxy m_metaProxy = new ManualConfiguredMetaProxy();

	@Override
	public synchronized void configure(ManualConfig newConfig) {
		if (newConfig != null && newConfig.getMeta() != null) {
			ManualConfig oldConfig = m_config.get();
			if (oldConfig == null || !oldConfig.equals(newConfig)) {
				newConfig.toBytes();
				m_metaProxy.setManualConfig(newConfig);
				m_config.set(newConfig);
				log.info("New manual config updated(version:{}).", newConfig.getVersion());
			}
		}
	}

	@Override
	public Meta getMeta() {
		ManualConfig config = m_config.get();
		return config == null ? null : config.getMeta();
	}

	@Override
	public MetaProxy getMetaProxy() {
		return getMeta() == null ? null : m_metaProxy;
	}

	@Override
	public synchronized void reset() {
		m_config.set(null);
		m_metaProxy.reset();
	}

	@Override
	public void initialize() throws InitializationException {

		registerConfigFetcher();

		checkManualConfigureMode();
		Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create("ManualConfigFetcher", true))
		      .scheduleWithFixedDelay(new Runnable() {

			      @Override
			      public void run() {
				      try {
					      checkManualConfigureMode();
				      } catch (Exception e) {
					      log.warn("Check manual config failed.");
				      }
			      }

		      }, 5, 5, TimeUnit.SECONDS);
	}

	private void registerConfigFetcher() throws InitializationException {
		Map<String, ManualConfigFetcher> configFetchers = lookupMap(ManualConfigFetcher.class);

		if (!configFetchers.isEmpty()) {
			if (configFetchers.size() == 1) {
				m_configFetcher = configFetchers.values().iterator().next();
			} else {
				for (Map.Entry<String, ManualConfigFetcher> entry : configFetchers.entrySet()) {
					if (!DefaultManualConfigFetcher.ID.equals(entry.getKey())) {
						m_configFetcher = entry.getValue();
						break;
					}
				}
			}
		}
		if (m_configFetcher == null) {
			throw new InitializationException("ManualConfigFetcher not found.");
		}
	}

	private void checkManualConfigureMode() {
		if (m_configProvider.isManualConfigureModeOn()) {
			if (getMeta() == null) {
				log.info("Entering manual config mode");
			}

			ManualConfig manualConfig = m_configFetcher.fetchConfig(getConfig());
			if (manualConfig != null) {
				configure(manualConfig);
			}
		} else {
			if (getMeta() != null) {
				log.info("Exiting from manual config mode");
				reset();
			}
		}
	}

	@Override
	public ManualConfig getConfig() {
		return m_config.get();
	}

	@Override
	public boolean isManualConfigModeOn() {
		return m_configProvider.isManualConfigureModeOn();
	}
}
