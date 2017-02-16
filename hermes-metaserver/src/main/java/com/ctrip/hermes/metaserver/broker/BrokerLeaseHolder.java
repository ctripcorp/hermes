package com.ctrip.hermes.metaserver.broker;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.net.Networks;
import org.unidal.tuple.Pair;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.constants.CatConstants;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.metaserver.commons.BaseLeaseHolder;
import com.ctrip.hermes.metaservice.model.BrokerLease;
import com.ctrip.hermes.metaservice.model.BrokerLeaseDao;
import com.ctrip.hermes.metaservice.model.BrokerLeaseEntity;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@SuppressWarnings("deprecation")
@Named(type = BrokerLeaseHolder.class)
public class BrokerLeaseHolder extends BaseLeaseHolder<Pair<String, Integer>> {

	private static final Logger log = LoggerFactory.getLogger(BrokerLeaseHolder.class);

	private static final int PERSIST_BATCH_SIZE = 100;
	
	public static final int LEASE_SYNC_INTERVAL_MILLIS = 2000;

	@Inject
	private BrokerLeaseDao m_leasesDao;

	@Override
	protected String getName() {
		return "BrokerLeaseHolder";
	}

	@Override
	public void close() {
		try {
			log.info("Closing broker lease holder ...");
			persistDirtyLeases(PERSIST_BATCH_SIZE);
		} catch (DalException e) {
			log.warn("Persist broker leases failed when closing broker lease holder.", e);
		}
	}

	@Override
	protected void doInitialize() throws Exception {
		List<BrokerLease> existingLeasesFromDB = null;

		int maxRetries = 3;
		for (int i = 0; i < maxRetries; i++) {
			try {
				existingLeasesFromDB = m_leasesDao.listLatestChanges(EMPTY_DATE, EMPTY_IP, BrokerLeaseEntity.READSET_FULL);
			} catch (Exception e) {
				log.error("[{}]Exception occurred while loading latest leases from db", getName(), e);
			}

			if (existingLeasesFromDB != null) {
				log.info("[{}]Existing leases loaded.", getName());
				break;
			} else {
				TimeUnit.SECONDS.sleep(1);
			}
		}

		long latestDataChangeTime = -1L;

		if (existingLeasesFromDB == null) {
			log.warn(
			      "[{}]Failed to load existing leases for {} times, will start metaserver without existing leases aware.",
			      getName(), maxRetries);
		} else {
			latestDataChangeTime = loadExistingLeases(existingLeasesFromDB);
		}

		startLeaseSynchronizationThread(latestDataChangeTime);
	}

	private void startLeaseSynchronizationThread(final long latestDataChangeTime) {
		Executors.newSingleThreadExecutor(HermesThreadFactory.create("BrokerLeaseSynchronizationThread", true)).submit(
		      new Runnable() {

			      @Override
			      public void run() {
				      long lastScanTime = latestDataChangeTime;

				      while (!Thread.currentThread().isInterrupted()) {
					      try {
						      persistDirtyLeases(PERSIST_BATCH_SIZE);
						      long maxDataTime = loadNewLeasesAssignedByOtherMetaservers(lastScanTime);
						      lastScanTime = Math.max(lastScanTime, maxDataTime);
					      } catch (Throwable e) {
						      log.error("Exception occurred in BrokerLeaseSynchronizationThread", e);
					      } finally {
						      try {
							      TimeUnit.MILLISECONDS.sleep(LEASE_SYNC_INTERVAL_MILLIS);
						      } catch (InterruptedException e) {
							      Thread.currentThread().interrupt();
						      }
					      }
				      }
			      }

		      });
	}

	private void persistDirtyLeases(int batchSize) throws DalException {
		List<List<BrokerLease>> leaseBatches = new LinkedList<>();
		List<BrokerLease> leaseBatch = null;
		Transaction transaction = Cat.newTransaction(CatConstants.TYPE_LEASE_DIRTY_PERSIST, "Broker");
		int persistCount = 0;
		try {
			for (Map.Entry<Pair<String, Integer>, LeasesContext> entry : m_leases.entrySet()) {
				LeasesContext leasesContext = entry.getValue();
				leasesContext.lock();
				try {
					if (leasesContext.isDirty()) {
						persistCount++;
						BrokerLease brokerLease = new BrokerLease();
						brokerLease.setTopic(entry.getKey().getKey());
						brokerLease.setPartition(entry.getKey().getValue());
						brokerLease.setMetaserver(Networks.forIp().getLocalHostAddress());
						brokerLease.setLeases(JSON.toJSONString(leasesContext.getLeasesMapping()));
						if (leaseBatch == null || leaseBatch.size() == batchSize) {
							leaseBatch = new ArrayList<>(batchSize);
							leaseBatches.add(leaseBatch);
						}
						leaseBatch.add(brokerLease);
						leasesContext.setDirty(false);
					}
				} finally {
					leasesContext.unlock();
				}
			}

			if (!leaseBatches.isEmpty()) {
				for (List<BrokerLease> batch : leaseBatches) {
					if (!batch.isEmpty()) {
						try {
							int[] updatedLeases = m_leasesDao.updateLeases(batch.toArray(new BrokerLease[batch.size()]),
							      BrokerLeaseEntity.UPDATESET_LEASES);

							int updatedCount = 0;
							for (int updated : updatedLeases) {
								updatedCount += updated;
							}

							if (updatedCount != batch.size()) {
								m_leasesDao.insertLeases(batch.toArray(new BrokerLease[batch.size()]));
							}

						} catch (Exception e) {
							log.error("[{}]Exception occurred while persisting latest leases to db", getName(), e);
						}
					}
				}
			}
			transaction.setStatus(Transaction.SUCCESS);
		} catch (Exception e) {
			transaction.setStatus(e);
			throw e;
		} finally {
			transaction.addData("count", persistCount);
			transaction.complete();
		}
	}

	private long loadNewLeasesAssignedByOtherMetaservers(long lastRunTime) throws DalException {
		Transaction transaction = Cat.newTransaction(CatConstants.TYPE_LEASE_DIRTY_LOAD, "Broker");
		int count = 0;
		long maxLodedTime = -1L;
		try {
			List<BrokerLease> changes = m_leasesDao.listLatestChanges(new Date(lastRunTime), Networks.forIp()
			      .getLocalHostAddress(), BrokerLeaseEntity.READSET_FULL);
			if (changes != null && !changes.isEmpty()) {
				count = changes.size();
				maxLodedTime = loadExistingLeases(changes);
			}
			transaction.setStatus(Transaction.SUCCESS);
			return maxLodedTime;
		} catch (Exception e) {
			transaction.setStatus(e);
			throw e;
		} finally {
			transaction.addData("count", count);
			transaction.complete();
		}
	}

	private long loadExistingLeases(List<BrokerLease> existingLeases) {
		long latestDataChangeTime = -1L;
		for (BrokerLease existingLease : existingLeases) {
			latestDataChangeTime = Math.max(latestDataChangeTime, existingLease.getDataChangeLastTime().getTime());
			Pair<String, Integer> contextKey = new Pair<>(existingLease.getTopic(), existingLease.getPartition());
			m_leases.putIfAbsent(contextKey, new LeasesContext());
			LeasesContext leasesContext = m_leases.get(contextKey);
			leasesContext.lock();
			try {
				if (!leasesContext.isDirty()) {
					leasesContext.setLeasesMapping(deserializeExistingLeases(existingLease.getLeases()));
				}
			} finally {
				leasesContext.unlock();
			}
		}

		return latestDataChangeTime;
	}
}
