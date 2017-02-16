package com.ctrip.hermes.metaserver.commons;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.metaserver.log.LoggerConstants;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public abstract class BaseLeaseHolder<Key> implements Initializable, LeaseHolder<Key> {

	private static final Logger log = LoggerFactory.getLogger(BaseLeaseHolder.class);

	private static final Logger traceLog = LoggerFactory.getLogger(LoggerConstants.TRACE);

	protected static final Date EMPTY_DATE = new Date(0L);

	protected static final String EMPTY_IP = "-----";

	private AtomicLong m_leaseIdGenerator = new AtomicLong(System.nanoTime());

	protected ConcurrentMap<Key, LeasesContext> m_leases = new ConcurrentHashMap<>();

	private AtomicBoolean m_inited = new AtomicBoolean(false);

	@Override
	public Map<Key, Map<String, ClientLeaseInfo>> getAllValidLeases() throws Exception {
		Map<Key, Map<String, ClientLeaseInfo>> validLeases = new HashMap<>();
		for (Map.Entry<Key, LeasesContext> entry : m_leases.entrySet()) {
			LeasesContext leasesContext = entry.getValue();
			leasesContext.lock();
			try {
				removeExpiredLeases(leasesContext);
				if (!leasesContext.getLeasesMapping().isEmpty()) {
					validLeases.put(entry.getKey(), leasesContext.getLeasesMapping());
				}
			} finally {
				leasesContext.unlock();
			}
		}

		return validLeases;
	}

	@Override
	public LeaseAcquireResponse executeLeaseOperation(Key contextKey, LeaseOperationCallback callback) throws Exception {
		LeasesContext leasesContext = m_leases.get(contextKey);
		if (leasesContext == null) {
			m_leases.putIfAbsent(contextKey, new LeasesContext());
			leasesContext = m_leases.get(contextKey);
		}
		leasesContext.lock();
		try {
			removeExpiredLeases(leasesContext);
			return callback.execute(leasesContext);
		} finally {
			leasesContext.unlock();
		}
	}

	@Override
	public Lease newLease(Key contextKey, String clientKey, LeasesContext leasesContext, long leaseTimeMillis,
	      String ip, int port) throws Exception {
		leasesContext.lock();
		try {
			Lease newLease = new Lease(m_leaseIdGenerator.incrementAndGet(), System.currentTimeMillis() + leaseTimeMillis);
			leasesContext.getLeasesMapping().put(clientKey, new ClientLeaseInfo(newLease, ip, port));
			leasesContext.setDirty(true);
			return newLease;
		} finally {
			leasesContext.unlock();
		}
	}

	@Override
	public boolean renewLease(Key contextKey, String clientKey, LeasesContext leasesContext,
	      ClientLeaseInfo existingLeaseInfo, long leaseTimeMillis, String ip, int port) throws Exception {
		leasesContext.lock();
		try {
			long newExpireTime = existingLeaseInfo.getLease().getExpireTime() + leaseTimeMillis;
			long now = System.currentTimeMillis();
			if (newExpireTime > now + 2 * leaseTimeMillis) {
				newExpireTime = now + 2 * leaseTimeMillis;
			}

			existingLeaseInfo.getLease().setExpireTime(newExpireTime);
			existingLeaseInfo.setIp(ip);
			existingLeaseInfo.setPort(port);
			leasesContext.getLeasesMapping().put(clientKey, existingLeaseInfo);
			leasesContext.setDirty(true);
			return true;
		} finally {
			leasesContext.unlock();
		}
	}

	protected void removeExpiredLeases(LeasesContext leasesContext) {
		if (leasesContext != null) {
			Iterator<Entry<String, ClientLeaseInfo>> iter = leasesContext.getLeasesMapping().entrySet().iterator();
			while (iter.hasNext()) {
				Entry<String, ClientLeaseInfo> entry = iter.next();
				Lease lease = entry.getValue().getLease();
				if (lease != null && lease.isExpired()) {
					iter.remove();
				}
			}
		}
	}

	@Override
	public void initialize() throws InitializationException {
		try {
			doInitialize();
			startDetailPrinterThread();
			m_inited.set(true);
		} catch (Exception e) {
			log.error("Failed to init LeaseHolder", e);
			throw new InitializationException("Failed to init LeaseHolder", e);
		}
	}

	protected abstract void doInitialize() throws Exception;

	private void startDetailPrinterThread() {
		Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create(getName() + "-DetailPrinter", true))
		      .scheduleWithFixedDelay(new Runnable() {

			      @Override
			      public void run() {
				      try {
					      Map<Key, Map<String, ClientLeaseInfo>> allValidLeases = getAllValidLeases();
					      if (traceLog.isInfoEnabled()) {
						      traceLog.info(getName() + "\n" + JSON.toJSONString(allValidLeases));
					      }
				      } catch (Exception e) {
					      // ignore
				      }
			      }

		      }, 1, 1, TimeUnit.MINUTES);

	}

	protected Map<String, ClientLeaseInfo> deserializeExistingLeases(String jsonStr) {
		try {
			if (!StringUtils.isBlank(jsonStr)) {
				return JSON.parseObject(jsonStr, new TypeReference<Map<String, ClientLeaseInfo>>() {
				}.getType());
			}
		} catch (Exception e) {
			log.error("Deserialize existing leases failed.", e);
		}
		return new HashMap<>();
	}

	@Override
	public boolean inited() {
		return m_inited.get();
	}

	protected abstract String getName();
	
	protected abstract void close();

}
