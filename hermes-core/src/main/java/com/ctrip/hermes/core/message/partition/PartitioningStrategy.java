package com.ctrip.hermes.core.message.partition;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface PartitioningStrategy {
	public int computePartitionNo(String key, int partitionCount);
}
