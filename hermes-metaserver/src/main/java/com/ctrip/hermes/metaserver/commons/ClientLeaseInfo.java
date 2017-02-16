package com.ctrip.hermes.metaserver.commons;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.ctrip.hermes.core.lease.Lease;

public class ClientLeaseInfo {
	private Lease m_lease;

	private AtomicReference<String> m_ip = new AtomicReference<>(null);

	private AtomicInteger m_port = new AtomicInteger();

	public ClientLeaseInfo() {
	}

	public ClientLeaseInfo(Lease lease, String ip, int port) {
		m_lease = lease;
		m_ip.set(ip);
		m_port.set(port);
	}

	public Lease getLease() {
		return m_lease;
	}

	public void setLease(Lease lease) {
		m_lease = lease;
	}

	public String getIp() {
		return m_ip.get();
	}

	public void setIp(String ip) {
		m_ip.set(ip);
	}

	public int getPort() {
		return m_port.get();
	}

	public void setPort(int port) {
		m_port.set(port);
	}

}
