package com.ctrip.hermes.metaserver.commons;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface LeaseHolder<Key> {

	public Map<Key, Map<String, ClientLeaseInfo>> getAllValidLeases() throws Exception;

	public LeaseAcquireResponse executeLeaseOperation(Key contextKey, LeaseOperationCallback callback) throws Exception;

	public Lease newLease(Key contextKey, String clientKey, LeasesContext leasesContext, long leaseTimeMillis,
	      String ip, int port) throws Exception;

	public boolean renewLease(Key contextKey, String clientKey, LeasesContext leasesContext,
	      ClientLeaseInfo existingLeaseInfo, long leaseTimeMillis, String ip, int port) throws Exception;

	public boolean inited();

	public static class LeasesContext {
		private Map<String, ClientLeaseInfo> m_leasesMapping = new HashMap<>();

		private boolean m_dirty = false;

		private Lock m_lock = new ReentrantLock();

		public Map<String, ClientLeaseInfo> getLeasesMapping() {
			return m_leasesMapping;
		}

		public void setLeasesMapping(Map<String, ClientLeaseInfo> leasesMapping) {
			m_leasesMapping = leasesMapping;
		}

		public void setDirty(boolean dirty) {
			m_dirty = dirty;
		}

		public boolean isDirty() {
			return m_dirty;
		}

		public void lock() {
			m_lock.lock();
		}

		public void unlock() {
			m_lock.unlock();
		}

	}

	public static interface LeaseOperationCallback {
		public LeaseAcquireResponse execute(LeasesContext leasesContext) throws Exception;
	}
}
