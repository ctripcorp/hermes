package com.ctrip.hermes.broker.queue.storage.mysql.dal;

public interface TopicPartitionAware {

	public String getTopic();

	public int getPartition();

}
