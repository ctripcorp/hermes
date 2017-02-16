package com.ctrip.hermes.broker.lease;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.broker.build.BuildConstants;
import com.ctrip.hermes.broker.lease.BrokerLeaseManager.BrokerLeaseKey;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;
import com.ctrip.hermes.core.lease.LeaseManager;
import com.ctrip.hermes.core.lease.SessionIdAware;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.env.config.broker.BrokerConfigProvider;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = LeaseManager.class, value = BuildConstants.BROKER)
public class BrokerLeaseManager implements LeaseManager<BrokerLeaseKey> {
	@Inject
	private MetaService m_metaService;

	@Inject
	private BrokerConfigProvider m_config;

	@Override
	public LeaseAcquireResponse tryAcquireLease(BrokerLeaseKey key) {
		return m_metaService.tryAcquireBrokerLease(key.getTopic(), key.getPartition(), key.getSessionId(),
		      m_config.getListeningPort());
	}

	@Override
	public LeaseAcquireResponse tryRenewLease(BrokerLeaseKey key, Lease lease) {
		return m_metaService.tryRenewBrokerLease(key.getTopic(), key.getPartition(), lease, key.getSessionId(),
		      m_config.getListeningPort());
	}

	public static class BrokerLeaseKey implements SessionIdAware {
		private String m_sessionId;

		private String m_topic;

		private int m_partition;

		public BrokerLeaseKey(String topic, int partition, String sessionId) {
			m_sessionId = sessionId;
			m_topic = topic;
			m_partition = partition;
		}

		@Override
		public String getSessionId() {
			return m_sessionId;
		}

		public String getTopic() {
			return m_topic;
		}

		public int getPartition() {
			return m_partition;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + m_partition;
			result = prime * result + ((m_sessionId == null) ? 0 : m_sessionId.hashCode());
			result = prime * result + ((m_topic == null) ? 0 : m_topic.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			BrokerLeaseKey other = (BrokerLeaseKey) obj;
			if (m_partition != other.m_partition)
				return false;
			if (m_sessionId == null) {
				if (other.m_sessionId != null)
					return false;
			} else if (!m_sessionId.equals(other.m_sessionId))
				return false;
			if (m_topic == null) {
				if (other.m_topic != null)
					return false;
			} else if (!m_topic.equals(other.m_topic))
				return false;
			return true;
		}

	}
}
