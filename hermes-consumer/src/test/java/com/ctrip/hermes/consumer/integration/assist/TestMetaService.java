package com.ctrip.hermes.consumer.integration.assist;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;

import com.ctrip.hermes.core.meta.internal.DefaultMetaService;
import com.ctrip.hermes.core.meta.internal.MetaProxy;
import com.ctrip.hermes.meta.entity.Meta;

public class TestMetaService extends DefaultMetaService {

	private TestMetaHolder m_metaHolder;

	private MetaProxy m_metaProxy;

	public TestMetaService setMetaHolder(TestMetaHolder metaHolder) {
		m_metaHolder = metaHolder;
		return this;
	}

	public TestMetaService setMetaProxy(MetaProxy metaProxy) {
		m_metaProxy = metaProxy;
		return this;
	}

	@Override
	protected Meta getMeta() {
		return m_metaHolder.getMeta();
	}

	@Override
	protected MetaProxy getMetaProxy() {
		return m_metaProxy;
	}

	@Override
	public void initialize() throws InitializationException {
		// do nothing
	}

}
