package com.ctrip.hermes.broker.biz.logger;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.log.BizEvent;
import com.ctrip.hermes.core.log.FileBizLogger;
import com.ctrip.hermes.env.config.broker.BrokerConfigProvider;

@Named
public class BrokerFileBizLogger extends FileBizLogger {
	@Inject
	BrokerConfigProvider m_config;

	@Override
	public void log(BizEvent event) {
		if (m_config.isBizLoggerEnabled()) {
			super.log(event);
		}
	}
}
