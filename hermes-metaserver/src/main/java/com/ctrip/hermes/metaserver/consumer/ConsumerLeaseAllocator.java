package com.ctrip.hermes.metaserver.consumer;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface ConsumerLeaseAllocator {
	public LeaseAcquireResponse tryAcquireLease(Tpg tpg, String consumerName, String ip) throws Exception;

	public LeaseAcquireResponse tryRenewLease(Tpg tpg, String consumerName, long leaseId, String ip) throws Exception;

}
