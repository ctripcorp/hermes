package com.ctrip.hermes.consumer.engine.lease;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.consumer.build.BuildConstants;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;
import com.ctrip.hermes.core.lease.LeaseManager;
import com.ctrip.hermes.core.meta.MetaService;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = LeaseManager.class, value = BuildConstants.CONSUMER)
public class ConsumerLeaseManager implements LeaseManager<ConsumerLeaseKey> {
	@Inject
	private MetaService m_metaService;

	@Override
	public LeaseAcquireResponse tryAcquireLease(ConsumerLeaseKey key) {
		return m_metaService.tryAcquireConsumerLease(key.getTpg(), key.getSessionId());
	}

	@Override
	public LeaseAcquireResponse tryRenewLease(ConsumerLeaseKey key, Lease lease) {
		return m_metaService.tryRenewConsumerLease(key.getTpg(), lease, key.getSessionId());
	}
}
