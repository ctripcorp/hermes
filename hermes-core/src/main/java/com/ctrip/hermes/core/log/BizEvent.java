package com.ctrip.hermes.core.log;

import java.util.HashMap;
import java.util.Map;

public class BizEvent {

	private String m_eventType;

	private long m_eventTime;

	private Map<String, Object> m_datas = new HashMap<String, Object>();

	public BizEvent(String eventType) {
		m_eventType = eventType;
		m_eventTime = System.currentTimeMillis();
	}

	public BizEvent(String eventType, long eventTime) {
		m_eventType = eventType;
		m_eventTime = eventTime;
	}

	public BizEvent addData(String key, Object value) {
		m_datas.put(key, value);

		return this;
	}

	public String getEventType() {
		return m_eventType;
	}

	public long getEventTime() {
		return m_eventTime;
	}

	public Map<String, Object> getDatas() {
		return m_datas;
	}

}
