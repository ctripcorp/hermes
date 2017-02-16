package com.ctrip.hermes.core.message.partition;

import java.util.Random;

import org.unidal.lookup.annotation.Named;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = PartitioningStrategy.class)
public class HashPartitioningStrategy implements PartitioningStrategy {

	private Random m_random = new Random();

	@Override
	public int computePartitionNo(String key, int partitionCount) {

		if (key == null) {
			return m_random.nextInt(Integer.MAX_VALUE) % partitionCount;
		} else {
			return (key.hashCode() == Integer.MIN_VALUE ? 0 : Math.abs(key.hashCode())) % partitionCount;
		}
	}

}
