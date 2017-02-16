package com.ctrip.hermes.metaserver.consumer;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface ConsumerLeaseAllocatorLocator {

	public ConsumerLeaseAllocator findAllocator(String topicName, String consumerGroupName);
}
