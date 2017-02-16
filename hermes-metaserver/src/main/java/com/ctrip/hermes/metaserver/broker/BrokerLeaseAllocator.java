package com.ctrip.hermes.metaserver.broker;

import com.ctrip.hermes.core.lease.LeaseAcquireResponse;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface BrokerLeaseAllocator {
	public LeaseAcquireResponse tryAcquireLease(String topic, int partition, String brokerName, String ip, int port)
	      throws Exception;

	public LeaseAcquireResponse tryRenewLease(String topic, int partition, String brokerName, long leaseId, String ip,
	      int port) throws Exception;

}
