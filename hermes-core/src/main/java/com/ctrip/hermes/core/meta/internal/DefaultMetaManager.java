package com.ctrip.hermes.core.meta.internal;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.meta.remote.RemoteMetaLoader;
import com.ctrip.hermes.core.meta.remote.RemoteMetaProxy;
import com.ctrip.hermes.env.ClientEnvironment;
import com.ctrip.hermes.meta.entity.Meta;

@Named(type = MetaManager.class)
public class DefaultMetaManager extends ContainerHolder implements MetaManager, Initializable {

	private static final Logger log = LoggerFactory.getLogger(DefaultMetaManager.class);

	@Inject(RemoteMetaLoader.ID)
	private MetaLoader m_remoteMeta;

	@Inject
	private ClientEnvironment m_env;

	@Inject(RemoteMetaProxy.ID)
	private MetaProxy m_remoteMetaProxy;

	@Override
	public MetaProxy getMetaProxy() {
		return m_remoteMetaProxy;
	}

	@Override
	public Meta loadMeta() {
		return m_remoteMeta.load();
	}

	@Override
	public void initialize() throws InitializationException {
		log.info("Meta manager started");
	}
}
