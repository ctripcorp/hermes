package com.ctrip.hermes.metaservice.service;

import java.util.Map;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface ZookeeperService {

	public void deleteMetaServerAssignmentZkPath(String topicName);

	public void ensurePath(String path) throws Exception;

	public void persist(String path, byte[] data, String... touchPaths) throws Exception;

	public String queryData(String path) throws Exception;

	public void updateZkBaseMetaVersion(long version) throws Exception;

	void persistBulk(Map<String, byte[]> pathAndDatas) throws Exception;

}
