package com.ctrip.hermes.metaservice.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.DalNotFoundException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.metaservice.converter.MetaModelToEntityConverter;
import com.ctrip.hermes.metaservice.model.Meta;
import com.ctrip.hermes.metaservice.model.MetaDao;
import com.ctrip.hermes.metaservice.model.MetaEntity;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = MetaService.class)
public class DefaultMetaService implements MetaService {
	protected static final Logger m_logger = LoggerFactory.getLogger(DefaultMetaService.class);

	@Inject
	protected MetaDao m_metaDao;

	private Meta m_metaModel;

	private com.ctrip.hermes.meta.entity.Meta m_metaEntity;

	@Override
	public com.ctrip.hermes.meta.entity.Meta refreshMeta() throws DalException {
		try {
			m_metaModel = m_metaDao.findLatest(MetaEntity.READSET_FULL);
			m_metaEntity = MetaModelToEntityConverter.convert(m_metaModel);
		} catch (DalNotFoundException e) {
			m_logger.warn("find Latest Meta failed", e);
			m_metaEntity = new com.ctrip.hermes.meta.entity.Meta();
			m_metaEntity.addStorage(new com.ctrip.hermes.meta.entity.Storage(com.ctrip.hermes.meta.entity.Storage.MYSQL));
			m_metaEntity.addStorage(new com.ctrip.hermes.meta.entity.Storage(com.ctrip.hermes.meta.entity.Storage.KAFKA));
			m_metaEntity.setVersion(0L);
		}
		return m_metaEntity;
	}

	public com.ctrip.hermes.meta.entity.Meta getMetaEntity() {
		if (m_metaModel == null || m_metaEntity == null) {
			try {
				refreshMeta();
			} catch (DalException e) {
				m_logger.warn("get meta entity failed", e);
				return new com.ctrip.hermes.meta.entity.Meta();
			}
		}
		return m_metaEntity;
	}

	protected boolean isMetaUpdated() {
		try {
			Meta latestMeta = m_metaDao.findLatest(MetaEntity.READSET_FULL);
			if (m_metaModel == null || latestMeta.getVersion() > m_metaModel.getVersion()) {
				m_metaModel = latestMeta;
				m_metaEntity = MetaModelToEntityConverter.convert(m_metaModel);
				return true;
			}
		} catch (DalException e) {
			m_logger.warn("Update meta from db failed.", e);
		}
		return false;
	}
}
