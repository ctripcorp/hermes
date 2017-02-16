package com.ctrip.hermes.metaserver.broker;

import java.util.Collection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.metaserver.commons.Assignment;
import com.ctrip.hermes.metaserver.commons.ClientLeaseInfo;
import com.ctrip.hermes.metaserver.commons.LeaseHolder.LeaseOperationCallback;
import com.ctrip.hermes.metaserver.commons.LeaseHolder.LeasesContext;
import com.ctrip.hermes.metaserver.config.MetaServerConfig;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = BrokerLeaseAllocator.class)
public class DefaultBrokerLeaseAllocator implements BrokerLeaseAllocator {

	private static final Logger log = LoggerFactory.getLogger(DefaultBrokerLeaseAllocator.class);

	@Inject
	private MetaServerConfig m_config;

	@Inject
	private SystemClockService m_systemClockService;

	@Inject
	private BrokerLeaseHolder m_leaseHolder;

	@Inject
	private BrokerAssignmentHolder m_assignmentHolder;

	public void setConfig(MetaServerConfig config) {
		m_config = config;
	}

	public void setSystemClockService(SystemClockService systemClockService) {
		m_systemClockService = systemClockService;
	}

	public void setLeaseHolder(BrokerLeaseHolder leaseHolder) {
		m_leaseHolder = leaseHolder;
	}

	public void setAssignmentHolder(BrokerAssignmentHolder assignmentHolder) {
		m_assignmentHolder = assignmentHolder;
	}

	@Override
	public LeaseAcquireResponse tryAcquireLease(String topic, int partition, String brokerName, String ip, int port)
	      throws Exception {

		Assignment<Integer> topicAssignment = m_assignmentHolder.getAssignment(topic);
		if (topicAssignment == null) {
			return topicNoAssignment();
		} else {
			if (topicAssignment.isAssignTo(partition, brokerName)) {
				return acquireLease(topic, partition, brokerName, ip, port);
			} else {
				return topicPartitionNotAssignToBroker(topic, partition);
			}
		}
	}

	@Override
	public LeaseAcquireResponse tryRenewLease(String topic, int partition, String brokerName, long leaseId, String ip,
	      int port) throws Exception {

		Assignment<Integer> topicAssignment = m_assignmentHolder.getAssignment(topic);
		if (topicAssignment == null) {
			return topicNoAssignment();
		} else {
			if (topicAssignment.isAssignTo(partition, brokerName)) {
				return renewLease(topic, partition, brokerName, leaseId, ip, port);
			} else {
				return topicPartitionNotAssignToBroker(topic, partition);
			}
		}
	}

	protected LeaseAcquireResponse topicNoAssignment() {
		return new LeaseAcquireResponse(false, null, m_systemClockService.now()
		      + m_config.getDefaultLeaseAcquireOrRenewRetryDelayMillis());
	}

	protected LeaseAcquireResponse topicPartitionNotAssignToBroker(String topic, int partition) throws Exception {
		return m_leaseHolder.executeLeaseOperation(new Pair<String, Integer>(topic, partition),
		      new LeaseOperationCallback() {

			      @Override
			      public LeaseAcquireResponse execute(LeasesContext leasesContext) throws Exception {
				      if (leasesContext.getLeasesMapping().isEmpty()) {
					      return new LeaseAcquireResponse(false, null, m_systemClockService.now()
					            + m_config.getDefaultLeaseAcquireOrRenewRetryDelayMillis());
				      } else {
					      Collection<ClientLeaseInfo> leases = leasesContext.getLeasesMapping().values();
					      // use the first lease's exp time
					      return new LeaseAcquireResponse(false, null, leases.iterator().next().getLease().getExpireTime());
				      }
			      }

		      });

	}

	protected LeaseAcquireResponse acquireLease(final String topic, final int partition, final String brokerName,
	      final String ip, final int port) throws Exception {
		final Pair<String, Integer> contextKey = new Pair<String, Integer>(topic, partition);

		return m_leaseHolder.executeLeaseOperation(contextKey, new LeaseOperationCallback() {

			@Override
			public LeaseAcquireResponse execute(LeasesContext leasesContext) throws Exception {

				if (leasesContext.getLeasesMapping().isEmpty()) {
					Lease newLease = m_leaseHolder.newLease(contextKey, brokerName, leasesContext,
					      m_config.getBrokerLeaseTimeMillis(), ip, port);

					if (newLease != null) {
						if (log.isDebugEnabled()) {
							log.debug("Acquire lease success(topic={}, partition={}, brokerName={}, leaseExpTime={}).", topic,
							      partition, brokerName, newLease.getExpireTime());
						}

						return new LeaseAcquireResponse(true, new Lease(newLease.getId(), newLease.getExpireTime()
						      + m_config.getBrokerLeaseClientSideAdjustmentTimeMills()), -1);
					} else {
						return new LeaseAcquireResponse(false, null, m_systemClockService.now()
						      + m_config.getDefaultLeaseAcquireOrRenewRetryDelayMillis());
					}
				} else {
					ClientLeaseInfo existingClientLeaseInfo = null;

					for (Map.Entry<String, ClientLeaseInfo> entry : leasesContext.getLeasesMapping().entrySet()) {
						ClientLeaseInfo clientLeaseInfo = entry.getValue();
						String leaseBrokerName = entry.getKey();
						if (leaseBrokerName.equals(brokerName)) {
							existingClientLeaseInfo = clientLeaseInfo;
							break;
						}
					}

					if (existingClientLeaseInfo != null) {
						return new LeaseAcquireResponse(true, new Lease(existingClientLeaseInfo.getLease().getId(),
						      existingClientLeaseInfo.getLease().getExpireTime()
						            + m_config.getBrokerLeaseClientSideAdjustmentTimeMills()), -1);
					} else {
						Collection<ClientLeaseInfo> leases = leasesContext.getLeasesMapping().values();
						// use the first lease's exp time
						return new LeaseAcquireResponse(false, null, leases.iterator().next().getLease().getExpireTime());
					}
				}

			}

		});

	}

	protected LeaseAcquireResponse renewLease(final String topic, final int partition, final String brokerName,
	      final long leaseId, final String ip, final int port) throws Exception {

		final Pair<String, Integer> contextKey = new Pair<String, Integer>(topic, partition);

		return m_leaseHolder.executeLeaseOperation(contextKey, new LeaseOperationCallback() {

			@Override
			public LeaseAcquireResponse execute(LeasesContext leasesContext) throws Exception {
				if (leasesContext.getLeasesMapping().isEmpty()) {
					return new LeaseAcquireResponse(false, null, m_systemClockService.now()
					      + m_config.getDefaultLeaseAcquireOrRenewRetryDelayMillis());
				} else {
					ClientLeaseInfo existingClientLeaseInfo = null;

					for (Map.Entry<String, ClientLeaseInfo> entry : leasesContext.getLeasesMapping().entrySet()) {
						ClientLeaseInfo clientLeaseInfo = entry.getValue();
						Lease lease = clientLeaseInfo.getLease();
						String leaseBrokerName = entry.getKey();
						if (lease != null && lease.getId() == leaseId && leaseBrokerName.equals(brokerName)) {
							existingClientLeaseInfo = clientLeaseInfo;
							break;
						}
					}

					if (existingClientLeaseInfo != null) {
						if (m_leaseHolder.renewLease(contextKey, brokerName, leasesContext, existingClientLeaseInfo,
						      m_config.getBrokerLeaseTimeMillis(), ip, port)) {
							if (log.isDebugEnabled()) {
								log.debug("Renew lease success(topic={}, partition={}, brokerName={}, leaseExpTime={}).",
								      topic, partition, brokerName, existingClientLeaseInfo.getLease().getExpireTime());
							}

							return new LeaseAcquireResponse(true, new Lease(leaseId, existingClientLeaseInfo.getLease()
							      .getExpireTime() + m_config.getBrokerLeaseClientSideAdjustmentTimeMills()), -1L);
						} else {
							return new LeaseAcquireResponse(false, null, m_systemClockService.now()
							      + m_config.getDefaultLeaseAcquireOrRenewRetryDelayMillis());
						}
					} else {
						Collection<ClientLeaseInfo> leases = leasesContext.getLeasesMapping().values();
						// use the first lease's exp time
						return new LeaseAcquireResponse(false, null, leases.iterator().next().getLease().getExpireTime());
					}
				}
			}

		});

	}
}
