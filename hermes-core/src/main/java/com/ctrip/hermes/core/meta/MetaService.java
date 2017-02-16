package com.ctrip.hermes.core.meta;

import java.util.List;
import java.util.Map;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;
import com.ctrip.hermes.core.message.retry.RetryPolicy;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Idc;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.meta.entity.ZookeeperEnsemble;

/**
 * 
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface MetaService {

	Pair<Endpoint, Long> findEndpointByTopicAndPartition(String topic, int partition);

	Partition findPartitionByTopicAndPartition(String topic, int partition);

	RetryPolicy findRetryPolicyByTopicAndGroup(String topic, String groupId);

	Storage findStorageByTopic(String topic);

	Topic findTopicByName(String topic);

	int getAckTimeoutSecondsByTopicAndConsumerGroup(String topic, String groupId);

	List<Datasource> listAllMysqlDataSources();

	List<Partition> listPartitionsByTopic(String topic);

	List<Topic> listTopicsByPattern(String topicPattern);

	LeaseAcquireResponse tryRenewBrokerLease(String topic, int partition, Lease lease, String sessionId, int brokerPort);

	int translateToIntGroupId(String topic, String groupId);

	LeaseAcquireResponse tryAcquireBrokerLease(String topic, int partition, String sessionId, int brokerPort);

	LeaseAcquireResponse tryAcquireConsumerLease(Tpg tpg, String sessionId);

	LeaseAcquireResponse tryRenewConsumerLease(Tpg tpg, Lease lease, String sessionId);

	boolean containsEndpoint(Endpoint endpoint);

	boolean containsConsumerGroup(String topicName, String groupId);

	Offset findMessageOffsetByTime(String topic, int partition, long time);

	Map<Integer, Offset> findMessageOffsetByTime(String topicName, long time);

	List<ZookeeperEnsemble> listAllZookeeperEnsemble();
	
	Idc getPrimaryIdc();
}
