package com.ctrip.hermes.broker.lease;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.broker.build.BuildConstants;
import com.ctrip.hermes.broker.lease.BrokerLeaseManager.BrokerLeaseKey;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;
import com.ctrip.hermes.core.lease.LeaseManager;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.env.config.broker.BrokerConfigProvider;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = BrokerLeaseContainer.class)
public class BrokerLeaseContainer implements Initializable {

	private static final Logger log = LoggerFactory.getLogger(BrokerLeaseContainer.class);

	@Inject(BuildConstants.BROKER)
	private LeaseManager<BrokerLeaseKey> m_leaseManager;

	@Inject
	private BrokerConfigProvider m_config;

	@Inject
	private SystemClockService m_systemClockService;

	private Map<BrokerLeaseKey, Lease> m_existingLeases = new ConcurrentHashMap<>();

	private Map<BrokerLeaseKey, Long> m_nextAcquireTimes = new ConcurrentHashMap<>();

	private ConcurrentMap<BrokerLeaseKey, AtomicBoolean> m_leaseAcquireTaskRunnings = new ConcurrentHashMap<>();

	private ScheduledExecutorService m_scheduledExecutorService;

	public Lease acquireLease(String topic, int partition, String sessionId) {
		BrokerLeaseKey key = new BrokerLeaseKey(topic, partition, sessionId);
		Lease lease = m_existingLeases.get(key);
		if (lease == null || lease.isExpired()) {
			scheduleLeaseAcquireTask(key);
			return null;
		} else {
			return lease;
		}
	}

	private void scheduleLeaseAcquireTask(final BrokerLeaseKey key) {
		if (!m_leaseAcquireTaskRunnings.containsKey(key)) {
			m_leaseAcquireTaskRunnings.putIfAbsent(key, new AtomicBoolean(false));
		}

		final AtomicBoolean acquireTaskRunning = m_leaseAcquireTaskRunnings.get(key);
		if (acquireTaskRunning.compareAndSet(false, true)) {

			m_scheduledExecutorService.submit(new Runnable() {

				@Override
				public void run() {
					try {
						doAcquireLease(key);
					} finally {
						acquireTaskRunning.set(false);
					}
				}

			});
		}

	}

	private void doAcquireLease(BrokerLeaseKey key) {
		Lease existingLease = m_existingLeases.get(key);
		if (existingLease != null && !existingLease.isExpired()) {
			return;
		}

		if (m_nextAcquireTimes.containsKey(key) && m_nextAcquireTimes.get(key) > m_systemClockService.now()) {
			return;
		}

		LeaseAcquireResponse response = m_leaseManager.tryAcquireLease(key);

		if (response != null && response.isAcquired()) {
			Lease lease = response.getLease();
			if (!lease.isExpired()) {
				m_existingLeases.put(key, lease);

				long renewDelay = calRenewDelay(lease);
				m_nextAcquireTimes.remove(key);
				scheduleRenewLeaseTask(key, renewDelay);
				log.info("Lease acquired(topic={}, partition={}, sessionId={}, leaseId={}, expireTime={})", key.getTopic(),
				      key.getPartition(), key.getSessionId(), lease.getId(), new Date(lease.getExpireTime()));
			}
		} else {
			long nextTryTime = response != null && response.getNextTryTime() > 0 ? response.getNextTryTime() : m_config
			      .getDefaultLeaseAcquireDelayMillis() + m_systemClockService.now();

			m_nextAcquireTimes.put(key, nextTryTime);
			if (log.isDebugEnabled()) {
				log.debug("Unable to acquire lease(topic={}, partition={}, sessionId={}), next retry time will be {}.",
				      key.getTopic(), key.getPartition(), key.getSessionId(), new Date(nextTryTime));
			}
		}
	}

	private long calRenewDelay(Lease lease) {
		return lease.getRemainingTime() - m_config.getLeaseRenewTimeMillsBeforeExpire();
	}

	private void scheduleRenewLeaseTask(final BrokerLeaseKey key, long delay) {
		if (delay < 0) {
			return;
		}

		m_scheduledExecutorService.schedule(new Runnable() {

			@Override
			public void run() {
				Lease existingLease = m_existingLeases.get(key);
				if (existingLease != null && !existingLease.isExpired()) {
					LeaseAcquireResponse response = m_leaseManager.tryRenewLease(key, existingLease);

					long renewDelay = 0;
					if (response != null && response.isAcquired()) {
						Lease lease = response.getLease();
						existingLease.setExpireTime(lease.getExpireTime());
						renewDelay = calRenewDelay(lease);
						if (log.isDebugEnabled()) {
							log.debug(
							      "Successfully renew lease(topic={}, partition={}, sessionId={}, leaseId={}, expireTime={})",
							      key.getTopic(), key.getPartition(), key.getSessionId(), existingLease.getId(), new Date(
							            existingLease.getExpireTime()));
						}
					} else {
						renewDelay = m_config.getDefaultLeaseRenewDelayMillis();
						if (response != null && response.getNextTryTime() > 0) {
							renewDelay = response.getNextTryTime() - m_systemClockService.now();
						}
						if (log.isDebugEnabled()) {
							log.debug(
							      "Unable to renew lease((topic={}, partition={}, sessionId={}, leaseId={}), next retry will delay {} milliseconds.",
							      key.getTopic(), key.getPartition(), key.getSessionId(), existingLease.getId(), renewDelay);
						}
					}
					scheduleRenewLeaseTask(key, renewDelay);
				} else {
					if (existingLease != null) {
						log.info("Lease expired(topic={}, parition={}, sessionId={}, leaseId={}).", key.getTopic(),
						      key.getPartition(), key.getSessionId(), existingLease.getId());
					} else {
						log.info("Lease expired(topic={}, parition={}, sessionId={}).", key.getTopic(), key.getPartition(),
						      key.getSessionId());
					}
				}
			}
		}, delay, TimeUnit.MILLISECONDS);

	}

	@Override
	public void initialize() throws InitializationException {
		m_scheduledExecutorService = Executors.newScheduledThreadPool(m_config.getLeaseContainerThreadCount(),
		      HermesThreadFactory.create("BrokerLeaseContainer", true));
	}

	public boolean isAllLeaseExpired() {
		for (Lease existingLease : m_existingLeases.values()) {
			if (!existingLease.isExpired()) {
				return false;
			}
		}
		return true;
	}
}
