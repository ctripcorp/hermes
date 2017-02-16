package com.ctrip.hermes.consumer.engine;

import com.ctrip.hermes.consumer.api.MessageListenerConfig;

public class FilterMessageListenerConfig extends MessageListenerConfig {
	private String m_filter;

	public String getFilter() {
		return m_filter;
	}

	public void setFilter(String filter) {
		m_filter = filter;
	}
}
