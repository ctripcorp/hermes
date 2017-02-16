package com.ctrip.hermes.metaserver.commons;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class ClientContext {
	private String m_name;

	private String m_ip;

	private int m_port;

	private long m_lastHeartbeatTime;

	private String m_group;

	private String m_idc;

	public ClientContext() {
	}

	public ClientContext(String name, String ip, int port, String group, String idc, long lastHeartbeatTime) {
		m_name = name;
		m_ip = ip;
		m_port = port;
		m_group = group;
		m_idc = idc;
		m_lastHeartbeatTime = lastHeartbeatTime;
	}

	public String getIdc() {
		return m_idc;
	}

	public void setIdc(String idc) {
		m_idc = idc;
	}

	public String getName() {
		return m_name;
	}

	public void setName(String name) {
		m_name = name;
	}

	public String getIp() {
		return m_ip;
	}

	public void setIp(String ip) {
		m_ip = ip;
	}

	public int getPort() {
		return m_port;
	}

	public void setPort(int port) {
		m_port = port;
	}

	public String getGroup() {
		return m_group;
	}

	public void setGroup(String group) {
		m_group = group;
	}

	public long getLastHeartbeatTime() {
		return m_lastHeartbeatTime;
	}

	public void setLastHeartbeatTime(long lastHeartbeatTime) {
		m_lastHeartbeatTime = lastHeartbeatTime;
	}

	@Override
	public String toString() {
		return "ClientContext [m_name=" + m_name + ", m_ip=" + m_ip + ", m_port=" + m_port + ", m_lastHeartbeatTime="
		      + m_lastHeartbeatTime + ", m_group=" + m_group + "]";
	}

}
