package com.ctrip.hermes.broker.queue.storage.mysql.dal;

public interface TopicPartitionPriorityAware extends TopicPartitionAware {

	public int getPriority();

}
