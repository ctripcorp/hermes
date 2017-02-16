package com.ctrip.hermes.metaservice.service;

import java.util.Map;

import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.framework.api.transaction.CuratorTransactionBridge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.metaservice.zk.ZKClient;
import com.ctrip.hermes.metaservice.zk.ZKPathUtils;
import com.ctrip.hermes.metaservice.zk.ZKSerializeUtils;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = ZookeeperService.class)
public class DefaultZookeeperService implements ZookeeperService {
	private static final Logger log = LoggerFactory.getLogger(DefaultZookeeperService.class);

	private static final int BULK_PERSISTENCE_MAX_SIZE = 200;

	@Inject
	private ZKClient m_zkClient;

	@Inject
	private SystemClockService m_systemClockService;

	public void setZkClient(ZKClient zkClient) {
		m_zkClient = zkClient;
	}

	public void setSystemClockService(SystemClockService systemClockService) {
		m_systemClockService = systemClockService;
	}

	@Override
	public void updateZkBaseMetaVersion(long version) throws Exception {
		ensurePath(ZKPathUtils.getBaseMetaVersionZkPath());

		m_zkClient.get().setData().forPath(ZKPathUtils.getBaseMetaVersionZkPath(), ZKSerializeUtils.serialize(version));
	}

	@Override
	public String queryData(String path) throws Exception {
		try {
			ensurePath(path);
			return ZKSerializeUtils.deserialize(m_zkClient.get().getData().forPath(path), String.class);
		} catch (Exception e) {
			log.error("Query zookeeper data failed:{}", path, e);
			throw e;
		}
	}

	@Override
	public void persist(String path, byte[] data, String... touchPaths) throws Exception {
		try {
			ensurePath(path);

			if (touchPaths != null && touchPaths.length > 0) {
				for (String touchPath : touchPaths) {
					ensurePath(touchPath);
				}
			}

			CuratorTransactionBridge curatorTransactionBridge = m_zkClient.get().inTransaction().setData()
			      .forPath(path, data);

			byte[] now = ZKSerializeUtils.serialize(m_systemClockService.now());
			if (touchPaths != null && touchPaths.length > 0) {
				for (String touchPath : touchPaths) {
					curatorTransactionBridge.and().setData().forPath(touchPath, now);
				}
			}

			curatorTransactionBridge.and().commit();

		} catch (Exception e) {
			log.error("Exception occurred in persist", e);
			throw e;
		}
	}

	@Override
	public void persistBulk(Map<String, byte[]> pathAndDatas) throws Exception {
		if (pathAndDatas != null && !pathAndDatas.isEmpty()) {
			try {
				CuratorTransaction transaction = null;
				CuratorTransactionBridge bridge = null;

				int uncommittedCount = 0;

				for (Map.Entry<String, byte[]> pathAndData : pathAndDatas.entrySet()) {
					String path = pathAndData.getKey();
					byte[] data = pathAndData.getValue();

					ensurePath(path);

					if (transaction == null) {
						transaction = m_zkClient.get().inTransaction();
						bridge = transaction.setData().forPath(path, data);
					} else {
						bridge.and().setData().forPath(path, data);
					}

					uncommittedCount++;

					if (uncommittedCount % BULK_PERSISTENCE_MAX_SIZE == 0) {
						bridge.and().commit();
						transaction = null;
						bridge = null;
						uncommittedCount = 0;
					}
				}

				if (uncommittedCount > 0) {
					bridge.and().commit();
				}
			} catch (Exception e) {
				log.error("Exception occurred in persist", e);
				throw e;
			}
		}
	}

	public void ensurePath(String path) throws Exception {
		m_zkClient.get().createContainers(path);
	}

	@Override
	public void deleteMetaServerAssignmentZkPath(String topicName) {
		byte[] now = ZKSerializeUtils.serialize(m_systemClockService.now());

		try {
			ensurePath(ZKPathUtils.getMetaServerAssignmentZkPath(topicName));

			m_zkClient.get().inTransaction()
			//
			      .delete().forPath(ZKPathUtils.getMetaServerAssignmentZkPath(topicName))
			      //
			      .and().setData().forPath(ZKPathUtils.getMetaServerAssignmentRootZkPath(), now)//
			      .and().commit();
		} catch (Exception e) {
			log.error("Exception occurred in deleteMetaServerAssignmentZkPath", e);
			throw new RuntimeException(e);
		}
	}

}
