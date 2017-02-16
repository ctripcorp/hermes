package com.ctrip.hermes.core.lease;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class LeaseAcquireResponse {
	private boolean m_acquired = false;

	private Lease m_lease;

	private long m_nextTryTime;

	public LeaseAcquireResponse() {
	}

	public LeaseAcquireResponse(boolean acquired, Lease lease, long nextTryTime) {
		m_acquired = acquired;
		m_lease = lease;
		m_nextTryTime = nextTryTime;
	}

	public boolean isAcquired() {
		return m_acquired;
	}

	public void setAcquired(boolean acquired) {
		m_acquired = acquired;
	}

	public Lease getLease() {
		return m_lease;
	}

	public void setLease(Lease lease) {
		m_lease = lease;
	}

	public long getNextTryTime() {
		return m_nextTryTime;
	}

	public void setNextTryTime(long nextTryTime) {
		m_nextTryTime = nextTryTime;
	}
	
	public LeaseAcquireResponse toClientLeaseResponse(long timeDiff) {
		if (timeDiff != Long.MIN_VALUE) {
			if (m_acquired && m_lease != null) {
				m_lease.setExpireTime(m_lease.getExpireTime() + timeDiff);
			} else if (m_nextTryTime != -1){
				m_nextTryTime += timeDiff;
			}
		}
		return this;
	}

}
