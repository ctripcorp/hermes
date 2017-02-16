package com.ctrip.hermes.core.meta.internal;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;
import com.ctrip.hermes.meta.entity.Meta;

/**
 * for test only
 * 
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = MetaProxy.class, value = LocalMetaProxy.ID)
public class LocalMetaProxy implements MetaProxy {

	public final static String ID = "local";

	private AtomicLong m_leaseId = new AtomicLong(0);

	@Override
	public LeaseAcquireResponse tryAcquireConsumerLease(Tpg tpg, String sessionId) {
		long expireTime = System.currentTimeMillis() + 10 * 1000L;
		long leaseId = m_leaseId.incrementAndGet();
		return new LeaseAcquireResponse(true, new Lease(leaseId, expireTime), expireTime);
	}

	@Override
	public LeaseAcquireResponse tryRenewConsumerLease(Tpg tpg, Lease lease, String sessionId) {
		return new LeaseAcquireResponse(false, null, System.currentTimeMillis() + 10 * 1000L);
	}

	@Override
	public LeaseAcquireResponse tryRenewBrokerLease(String topic, int partition, Lease lease, String sessionId,
	      int brokerPort) {
		long expireTime = System.currentTimeMillis() + 10 * 1000L;
		long leaseId = m_leaseId.incrementAndGet();
		return new LeaseAcquireResponse(true, new Lease(leaseId, expireTime), expireTime);
	}

	@Override
	public LeaseAcquireResponse tryAcquireBrokerLease(String topic, int partition, String sessionId, int brokerPort) {
		long expireTime = System.currentTimeMillis() + 10 * 1000L;
		long leaseId = m_leaseId.incrementAndGet();
		return new LeaseAcquireResponse(true, new Lease(leaseId, expireTime), expireTime);
	}

	@Override
	public int registerSchema(String schema, String subject) {
		return -1;
	}

	@Override
	public String getSchemaString(int schemaId) {
		return null;
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
		return null;
	}
}
