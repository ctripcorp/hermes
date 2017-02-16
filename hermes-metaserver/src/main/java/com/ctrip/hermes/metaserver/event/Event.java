package com.ctrip.hermes.metaserver.event;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class Event {
	private EventType m_type;

	private long m_version;

	private Object m_data;

	public Event(EventType type, long version, Object data) {
		m_type = type;
		m_version = version;
		m_data = data;
	}

	public EventType getType() {
		return m_type;
	}

	public long getVersion() {
		return m_version;
	}

	public Object getData() {
		return m_data;
	}

}
