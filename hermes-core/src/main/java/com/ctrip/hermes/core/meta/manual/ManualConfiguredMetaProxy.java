package com.ctrip.hermes.core.meta.manual;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.bo.Tp;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;
import com.ctrip.hermes.core.meta.internal.MetaProxy;
import com.ctrip.hermes.core.meta.manual.ManualConfig.ManualBrokerAssignemnt;
import com.ctrip.hermes.core.meta.manual.ManualConfig.ManualConsumerAssignemnt;
import com.ctrip.hermes.meta.entity.Meta;

class ManualConfiguredMetaProxy implements MetaProxy {

	private static final long LEASE_EXPIRE_TIME_MILLIS = 5000;

	private static final long NO_LEASE_DELAY_MILLIS = 1000;

	private static final AtomicLong LEASE_ID_GENERATOR = new AtomicLong();

	private AtomicReference<Meta> m_meta = new AtomicReference<>();

	private AtomicReference<Map<Tpg, String>> m_consumerAssignments = new AtomicReference<Map<Tpg, String>>(
	      new HashMap<Tpg, String>());

	private Map<Tpg, Pair<String, Lease>> m_existingConsumerLeases = new ConcurrentHashMap<>();

	private AtomicReference<Map<Tp, String>> m_brokerAssignments = new AtomicReference<Map<Tp, String>>(
	      new HashMap<Tp, String>());

	private Map<Tp, Pair<String, Lease>> m_existingBrokerLeases = new ConcurrentHashMap<>();

	public void setManualConfig(ManualConfig newConfig) {
		m_meta.set(newConfig.getMeta());
		extractConsumerAssignments(newConfig);
		extractBrokerAssignments(newConfig);
	}

	public void reset() {
		m_meta.set(null);
		m_existingConsumerLeases.clear();
		m_existingBrokerLeases.clear();
		m_consumerAssignments.set(new HashMap<Tpg, String>());
		m_brokerAssignments.set(new HashMap<Tp, String>());
	}

	private void extractConsumerAssignments(ManualConfig newConfig) {
		Map<Tpg, String> newConsumerAssignments = new HashMap<>();
		Set<ManualConsumerAssignemnt> consumerAssignments = newConfig.getConsumerAssignments();
		for (ManualConsumerAssignemnt assignment : consumerAssignments) {
			String topic = assignment.getTopic();
			String group = assignment.getGroup();
			for (Map.Entry<String, List<Integer>> entry : assignment.getAssignment().entrySet()) {
				if (entry.getValue() != null) {
					String sessionId = entry.getKey();
					for (Integer partition : entry.getValue()) {
						Tpg tpg = new Tpg(topic, partition, group);
						newConsumerAssignments.put(tpg, sessionId);
					}
				}
			}
		}

		m_consumerAssignments.set(newConsumerAssignments);
	}

	private void extractBrokerAssignments(ManualConfig newConfig) {
		Map<Tp, String> newBrokerAssignments = new HashMap<>();
		Set<ManualBrokerAssignemnt> brokerAssignments = newConfig.getBrokerAssignments();
		for (ManualBrokerAssignemnt assignment : brokerAssignments) {
			String brokerId = assignment.getBrokerId();
			for (Map.Entry<String, List<Integer>> entry : assignment.getOwnedTopicPartitions().entrySet()) {
				if (entry.getValue() != null) {
					String topic = entry.getKey();
					for (Integer partition : entry.getValue()) {
						Tp tp = new Tp(topic, partition);
						newBrokerAssignments.put(tp, brokerId);
					}
				}
			}
		}

		m_brokerAssignments.set(newBrokerAssignments);
	}

	@Override
	public LeaseAcquireResponse tryAcquireConsumerLease(Tpg tpg, String sessionId) {
		String assignedSessionId = m_consumerAssignments.get().get(tpg);
		Pair<String, Lease> existingSessionLeasePair = m_existingConsumerLeases.get(tpg);
		if (assignedSessionId != null && assignedSessionId.equals(sessionId)) {
			if (existingSessionLeasePair == null || existingSessionLeasePair.getValue().isExpired()) {
				Lease newLease = new Lease(LEASE_ID_GENERATOR.incrementAndGet(), System.currentTimeMillis()
				      + LEASE_EXPIRE_TIME_MILLIS);
				m_existingConsumerLeases.put(tpg, new Pair<>(sessionId, newLease));
				return new LeaseAcquireResponse(true, newLease, -1L);
			} else {
				String leaseHoldingSessionId = existingSessionLeasePair.getKey();
				if (leaseHoldingSessionId.equals(sessionId)) {
					return new LeaseAcquireResponse(true, existingSessionLeasePair.getValue(), -1L);
				} else {
					return new LeaseAcquireResponse(false, null, existingSessionLeasePair.getValue().getExpireTime());
				}
			}
		} else {
			return new LeaseAcquireResponse(false, null, System.currentTimeMillis() + NO_LEASE_DELAY_MILLIS);
		}
	}

	@Override
	public LeaseAcquireResponse tryRenewConsumerLease(Tpg tpg, Lease lease, String sessionId) {
		String assignedSessionId = m_consumerAssignments.get().get(tpg);
		Pair<String, Lease> existingSessionLeasePair = m_existingConsumerLeases.get(tpg);
		if (assignedSessionId != null && assignedSessionId.equals(sessionId)) {
			if (existingSessionLeasePair == null || existingSessionLeasePair.getValue().isExpired()) {
				return new LeaseAcquireResponse(false, null, System.currentTimeMillis() + NO_LEASE_DELAY_MILLIS);
			} else {
				String leaseHoldingSessionId = existingSessionLeasePair.getKey();
				Lease existingLease = existingSessionLeasePair.getValue();
				if (leaseHoldingSessionId.equals(sessionId) && lease.getId() == existingLease.getId()) {
					existingLease.setExpireTime(Math.min(System.currentTimeMillis() + 2 * LEASE_EXPIRE_TIME_MILLIS,
					      existingLease.getExpireTime() + LEASE_EXPIRE_TIME_MILLIS));
					return new LeaseAcquireResponse(true, existingLease, -1L);
				} else {
					return new LeaseAcquireResponse(false, null, existingLease.getExpireTime());
				}
			}
		} else {
			return new LeaseAcquireResponse(false, null, System.currentTimeMillis() + NO_LEASE_DELAY_MILLIS);
		}
	}

	@Override
	public LeaseAcquireResponse tryRenewBrokerLease(String topic, int partition, Lease lease, String brokerId,
	      int brokerPort) {
		Tp tp = new Tp(topic, partition);
		String assignedBrokerId = m_brokerAssignments.get().get(tp);
		Pair<String, Lease> existingBrokerLeasePair = m_existingBrokerLeases.get(tp);
		if (assignedBrokerId != null && assignedBrokerId.equals(brokerId)) {
			if (existingBrokerLeasePair == null || existingBrokerLeasePair.getValue().isExpired()) {
				return new LeaseAcquireResponse(false, null, System.currentTimeMillis() + NO_LEASE_DELAY_MILLIS);
			} else {
				String leaseHoldingBrokerId = existingBrokerLeasePair.getKey();
				Lease existingLease = existingBrokerLeasePair.getValue();
				if (leaseHoldingBrokerId.equals(brokerId) && lease.getId() == existingLease.getId()) {
					existingLease.setExpireTime(Math.min(System.currentTimeMillis() + 2 * LEASE_EXPIRE_TIME_MILLIS,
					      existingLease.getExpireTime() + LEASE_EXPIRE_TIME_MILLIS));
					return new LeaseAcquireResponse(true, existingLease, -1L);
				} else {
					return new LeaseAcquireResponse(false, null, existingLease.getExpireTime());
				}
			}
		} else {
			return new LeaseAcquireResponse(false, null, System.currentTimeMillis() + NO_LEASE_DELAY_MILLIS);
		}
	}

	@Override
	public LeaseAcquireResponse tryAcquireBrokerLease(String topic, int partition, String brokerId, int brokerPort) {
		Tp tp = new Tp(topic, partition);
		String assignedBrokerId = m_brokerAssignments.get().get(tp);
		Pair<String, Lease> existingBrokerLeasePair = m_existingBrokerLeases.get(tp);
		if (assignedBrokerId != null && assignedBrokerId.equals(brokerId)) {
			if (existingBrokerLeasePair == null || existingBrokerLeasePair.getValue().isExpired()) {
				Lease newLease = new Lease(LEASE_ID_GENERATOR.incrementAndGet(), System.currentTimeMillis()
				      + LEASE_EXPIRE_TIME_MILLIS);
				m_existingBrokerLeases.put(tp, new Pair<>(brokerId, newLease));
				return new LeaseAcquireResponse(true, newLease, -1L);
			} else {
				String leaseHoldingBrokerId = existingBrokerLeasePair.getKey();
				if (leaseHoldingBrokerId.equals(brokerId)) {
					return new LeaseAcquireResponse(true, existingBrokerLeasePair.getValue(), -1L);
				} else {
					return new LeaseAcquireResponse(false, null, existingBrokerLeasePair.getValue().getExpireTime());
				}
			}
		} else {
			return new LeaseAcquireResponse(false, null, System.currentTimeMillis() + NO_LEASE_DELAY_MILLIS);
		}
	}

	@Override
	public int registerSchema(String schema, String subject) {
		throw new IllegalStateException("Working in manual config mode");
	}

	@Override
	public String getSchemaString(int schemaId) {
		throw new IllegalStateException("Working in manual config mode");
	}

	@Override
	public Map<Integer, Offset> findMessageOffsetByTime(String topic, int partition, long time) {
		return null;
	}

	@Override
	public Pair<Integer, String> getRequestToMetaServer(String path, Map<String, String> requestParams) {
		return null;
	}

	@Override
	public Meta getTopicsMeta(Set<String> topics) {
		return m_meta.get();
	}

}
