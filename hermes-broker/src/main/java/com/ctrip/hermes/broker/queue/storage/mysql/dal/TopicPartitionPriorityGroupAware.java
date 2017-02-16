package com.ctrip.hermes.broker.queue.storage.mysql.dal;

public interface TopicPartitionPriorityGroupAware extends TopicPartitionPriorityAware {

	public int getGroupId();

}
