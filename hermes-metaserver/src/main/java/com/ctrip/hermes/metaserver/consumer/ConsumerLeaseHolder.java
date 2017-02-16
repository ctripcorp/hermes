package com.ctrip.hermes.metaserver.consumer;

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

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.constants.CatConstants;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.metaserver.commons.BaseLeaseHolder;
import com.ctrip.hermes.metaservice.model.ConsumerLease;
import com.ctrip.hermes.metaservice.model.ConsumerLeaseDao;
import com.ctrip.hermes.metaservice.model.ConsumerLeaseEntity;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@SuppressWarnings("deprecation")
@Named(type = ConsumerLeaseHolder.class)
public class ConsumerLeaseHolder extends BaseLeaseHolder<Tpg> {
	private static final Logger log = LoggerFactory.getLogger(ConsumerLeaseHolder.class);

	private static final int PERSIST_BATCH_SIZE = 100;
	
	public static final int LEASE_SYNC_INTERVAL_MILLIS = 2000;

	@Inject
	private ConsumerLeaseDao m_leasesDao;

	@Override
	protected String getName() {
		return "ConsumerLeaseHolder";
	}

	@Override
	public void close() {
		try {
			log.info("Closing consumer lease holder ...");
			persistDirtyLeases(PERSIST_BATCH_SIZE);
		} catch (DalException e) {
			log.warn("Persist consumer leases failed when closing broker lease holder.", e);
		}
	}

	@Override
	protected void doInitialize() throws Exception {
		List<ConsumerLease> existingLeasesFromDB = null;

		int maxRetries = 3;
		for (int i = 0; i < maxRetries; i++) {
			try {
				existingLeasesFromDB = m_leasesDao
				      .listLatestChanges(EMPTY_DATE, EMPTY_IP, ConsumerLeaseEntity.READSET_FULL);
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
		Executors.newSingleThreadExecutor(HermesThreadFactory.create("ConsumerLeaseSynchronizationThread", true)).submit(
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
						      log.error("Exception occurred in ConsumerLeaseSynchronizationThread", e);
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

	private long loadNewLeasesAssignedByOtherMetaservers(long lastRunTime) throws DalException {
		Transaction transaction = Cat.newTransaction(CatConstants.TYPE_LEASE_DIRTY_LOAD, "Consumer");
		int count = 0;
		long maxLodedTime = -1L;
		try {
			List<ConsumerLease> changes = m_leasesDao.listLatestChanges(new Date(lastRunTime), Networks.forIp()
			      .getLocalHostAddress(), ConsumerLeaseEntity.READSET_FULL);
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

	private void persistDirtyLeases(int batchSize) throws DalException {
		List<List<ConsumerLease>> leaseBatches = new LinkedList<>();
		List<ConsumerLease> leaseBatch = null;
		Transaction transaction = Cat.newTransaction(CatConstants.TYPE_LEASE_DIRTY_PERSIST, "Consumer");
		int persistCount = 0;
		try {
			for (Map.Entry<Tpg, LeasesContext> entry : m_leases.entrySet()) {
				LeasesContext leasesContext = entry.getValue();
				leasesContext.lock();
				try {
					if (leasesContext.isDirty()) {
						persistCount++;
						ConsumerLease consumerLease = new ConsumerLease();
						consumerLease.setTopic(entry.getKey().getTopic());
						consumerLease.setPartition(entry.getKey().getPartition());
						consumerLease.setGroup(entry.getKey().getGroupId());
						consumerLease.setMetaserver(Networks.forIp().getLocalHostAddress());
						consumerLease.setLeases(JSON.toJSONString(leasesContext.getLeasesMapping()));
						if (leaseBatch == null || leaseBatch.size() == batchSize) {
							leaseBatch = new ArrayList<>(batchSize);
							leaseBatches.add(leaseBatch);
						}
						leaseBatch.add(consumerLease);
						leasesContext.setDirty(false);
					}
				} finally {
					leasesContext.unlock();
				}
			}

			if (!leaseBatches.isEmpty()) {
				for (List<ConsumerLease> batch : leaseBatches) {
					if (!batch.isEmpty()) {
						try {
							int[] updatedLeases = m_leasesDao.updateLeases(batch.toArray(new ConsumerLease[batch.size()]),
							      ConsumerLeaseEntity.UPDATESET_LEASES);

							int updatedCount = 0;
							for (int updated : updatedLeases) {
								updatedCount += updated;
							}

							if (updatedCount != batch.size()) {
								m_leasesDao.insertLeases(batch.toArray(new ConsumerLease[batch.size()]));
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

	private long loadExistingLeases(List<ConsumerLease> existingLeases) {
		long latestDataChangeTime = -1L;
		for (ConsumerLease existingLease : existingLeases) {
			latestDataChangeTime = Math.max(latestDataChangeTime, existingLease.getDataChangeLastTime().getTime());
			Tpg contextKey = new Tpg(existingLease.getTopic(), existingLease.getPartition(), existingLease.getGroup());
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
