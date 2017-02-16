package com.ctrip.hermes.metaservice.service;

import java.util.List;

import org.unidal.dal.jdbc.DalException;

import com.ctrip.hermes.meta.entity.ZookeeperEnsemble;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface ZookeeperEnsembleService {
	List<ZookeeperEnsemble> listEnsembles() throws DalException;

	List<com.ctrip.hermes.metaservice.model.ZookeeperEnsemble> listEnsembleModels() throws DalException;

	void addZookeeperEnsemble(com.ctrip.hermes.metaservice.model.ZookeeperEnsemble zookeeperEnsemble)
	      throws DalException;

	void updateZookeeperEnsemble(com.ctrip.hermes.metaservice.model.ZookeeperEnsemble zookeeperEnsemble)
	      throws DalException;

	void deleteZookeeperEnsemble(int zookeeperEnsembleId) throws DalException;

	void switchPrimaryZookeeperEnsemble(int zookeeperEnsembleId) throws DalException;
	
	com.ctrip.hermes.metaservice.model.ZookeeperEnsemble getPrimaryZookeeperEnsemble();

	com.ctrip.hermes.metaservice.model.ZookeeperEnsemble findZookeeperEnsembleModelById(int zookeeperEnsembleId);
	
}
