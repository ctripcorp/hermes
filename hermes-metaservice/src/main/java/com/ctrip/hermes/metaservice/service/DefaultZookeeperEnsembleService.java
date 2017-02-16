package com.ctrip.hermes.metaservice.service;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.transaction.TransactionManager;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.meta.entity.ZookeeperEnsemble;
import com.ctrip.hermes.metaservice.model.ZookeeperEnsembleDao;
import com.ctrip.hermes.metaservice.model.ZookeeperEnsembleEntity;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = ZookeeperEnsembleService.class)
public class DefaultZookeeperEnsembleService implements ZookeeperEnsembleService {

	private static final Logger log = LoggerFactory.getLogger(DefaultZookeeperEnsembleService.class);

	@Inject
	private ZookeeperEnsembleDao m_dao;

	@Inject
	private TransactionManager m_tm;

	private ZookeeperEnsemble toMetaEntity(com.ctrip.hermes.metaservice.model.ZookeeperEnsemble ensembleEntity) {
		ZookeeperEnsemble ensemble = new ZookeeperEnsemble();

		ensemble.setId(ensembleEntity.getName());
		ensemble.setConnectionString(StringUtils.trimToEmpty(ensembleEntity.getConnectionString()));
		ensemble.setIdc(ensembleEntity.getIdc());
		ensemble.setPrimary(ensembleEntity.isPrimary());

		return ensemble;
	}

	@Override
	public List<ZookeeperEnsemble> listEnsembles() throws DalException {
		List<ZookeeperEnsemble> ensembles = new ArrayList<>();

		List<com.ctrip.hermes.metaservice.model.ZookeeperEnsemble> ensembleEntities = m_dao
		      .list(ZookeeperEnsembleEntity.READSET_FULL);
		if (ensembleEntities != null && !ensembleEntities.isEmpty()) {
			for (com.ctrip.hermes.metaservice.model.ZookeeperEnsemble ensembleEntity : ensembleEntities) {
				ZookeeperEnsemble ensemble = toMetaEntity(ensembleEntity);
				if (!StringUtils.isEmpty(ensemble.getConnectionString())) {
					ensembles.add(ensemble);
				}
			}
		}

		return ensembles;
	}

	@Override
	public List<com.ctrip.hermes.metaservice.model.ZookeeperEnsemble> listEnsembleModels() throws DalException {
		return m_dao.list(ZookeeperEnsembleEntity.READSET_FULL);
	}

	@Override
	public void addZookeeperEnsemble(com.ctrip.hermes.metaservice.model.ZookeeperEnsemble zookeeperEnsemble)
	      throws DalException {
		m_dao.insert(zookeeperEnsemble);
	}

	@Override
	public void updateZookeeperEnsemble(com.ctrip.hermes.metaservice.model.ZookeeperEnsemble zookeeperEnsemble)
	      throws DalException {
		m_dao.updateByPK(zookeeperEnsemble, ZookeeperEnsembleEntity.UPDATESET_FULL);
	}

	@Override
	public void deleteZookeeperEnsemble(int ZookeeperEnsembleId) throws DalException {
		com.ctrip.hermes.metaservice.model.ZookeeperEnsemble zookeeperEnsemble = new com.ctrip.hermes.metaservice.model.ZookeeperEnsemble();
		zookeeperEnsemble.setId(ZookeeperEnsembleId);
		m_dao.deleteByPK(zookeeperEnsemble);
	}

	@Override
	public void switchPrimaryZookeeperEnsemble(int zookeeperEnsembleId) throws DalException {
		com.ctrip.hermes.metaservice.model.ZookeeperEnsemble zookeeperEnsemble = findZookeeperEnsembleModelById(zookeeperEnsembleId);
		if (zookeeperEnsemble == null) {
			throw new RuntimeException(String.format("Can not find zookeeperEnsemble(id=%s)", zookeeperEnsembleId));
		}

		m_tm.startTransaction("fxhermesmetadb");
		try {
			List<com.ctrip.hermes.metaservice.model.ZookeeperEnsemble> zookeeperEnsembles = m_dao
			      .list(ZookeeperEnsembleEntity.READSET_FULL);

			for (com.ctrip.hermes.metaservice.model.ZookeeperEnsemble zkEnsemble : zookeeperEnsembles) {
				if (zkEnsemble.isPrimary() && zkEnsemble.getId() != zookeeperEnsemble.getId()) {
					zkEnsemble.setPrimary(false);
					updateZookeeperEnsemble(zkEnsemble);
				} else if (!zkEnsemble.isPrimary() && zkEnsemble.getId() == zookeeperEnsemble.getId()) {
					zkEnsemble.setPrimary(true);
					updateZookeeperEnsemble(zkEnsemble);
				}
			}

			m_tm.commitTransaction();
		} catch (Exception e) {
			m_tm.rollbackTransaction();
			log.error("Failed to switch primary zookeeperEnsemble to {}", zookeeperEnsemble.getConnectionString(), e);
			throw e;
		}
	}

	@Override
	public com.ctrip.hermes.metaservice.model.ZookeeperEnsemble findZookeeperEnsembleModelById(int zookeeperEnsembleId) {
		List<com.ctrip.hermes.metaservice.model.ZookeeperEnsemble> zookeeperEnsemble;
		try {
			zookeeperEnsemble = m_dao.findById(zookeeperEnsembleId, ZookeeperEnsembleEntity.READSET_FULL);
		} catch (DalException e) {
			log.error("Failed to find zookeeper ensemble(id={}) from db.", zookeeperEnsembleId, e);
			throw new RuntimeException(String.format("Failed to find zookeeper ensemble(id=%s) from db.",
			      zookeeperEnsembleId), e);
		}
		if (zookeeperEnsemble.isEmpty()) {
			return null;
		} else {
			return zookeeperEnsemble.get(0);
		}
	}

	@Override
	public com.ctrip.hermes.metaservice.model.ZookeeperEnsemble getPrimaryZookeeperEnsemble() {
		List<com.ctrip.hermes.metaservice.model.ZookeeperEnsemble> primaryZk;
		try {
			primaryZk = m_dao.findPrimary(ZookeeperEnsembleEntity.READSET_FULL);
		} catch (DalException e) {
			log.error("Can not get primary zookeeper ensemble from db!", e);
			throw new RuntimeException(String.format("Can not get primary zookeeper ensemble from db for reason: %s",
			      e.getMessage()));
		}
		if (primaryZk.size() >= 1) {
			log.error("Existing {} primary zookeeper ensemble in db, will return the first one.", primaryZk.size());
		} else if (primaryZk.isEmpty()) {
			log.error("No primary zookeeper ensemble exists in db!");
			return null;
		}

		return primaryZk.get(0);
	}
}
