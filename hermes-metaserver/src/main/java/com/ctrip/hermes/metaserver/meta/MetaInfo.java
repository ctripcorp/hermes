package com.ctrip.hermes.metaserver.meta;

public class MetaInfo {

	private String m_host;

	private int m_port;

	private long m_timestamp;

	public MetaInfo() {
	}

	public MetaInfo(String host, int port, long timestamp) {
		m_host = host;
		m_port = port;
		m_timestamp = timestamp;
	}

	public String getHost() {
		return m_host;
	}

	public void setHost(String host) {
		m_host = host;
	}

	public int getPort() {
		return m_port;
	}

	public void setPort(int port) {
		m_port = port;
	}

	public long getTimestamp() {
		return m_timestamp;
	}

	public void setTimestamp(long timestamp) {
		m_timestamp = timestamp;
	}

}
