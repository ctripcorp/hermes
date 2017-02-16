package com.ctrip.hermes.core.message.partition;

import static org.junit.Assert.assertTrue;

import java.util.Random;

import org.junit.Before;
import org.junit.Test;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class HashPartitioningStrategyTest {
	private HashPartitioningStrategy strategy;

	private Random random = new Random();

	@Before
	public void before() throws Exception {
		strategy = new HashPartitioningStrategy();
	}

	@Test
	public void testNormal() throws Exception {
		for (int i = 0; i < 100000; i++) {
			int partitionNo = strategy.computePartitionNo(generateRandomString(100), 5);
			assertTrue(partitionNo < 5);
			assertTrue(partitionNo >= 0);
		}
	}

	@Test
	public void testNull() throws Exception {
		int partitionNo = strategy.computePartitionNo(null, 5);
		assertTrue(partitionNo < 5);
		assertTrue(partitionNo >= 0);
	}

	private String generateRandomString(int maxLen) {
		int len = random.nextInt(maxLen + 1);
		while (len == 0) {
			len = random.nextInt(maxLen + 1);
		}
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < len; i++) {
			char c = 'a';
			if (random.nextBoolean()) {
				c = (char) ('a' + random.nextInt(26));
			} else {
				c = (char) ('A' + random.nextInt(26));
			}
			sb.append(c);
		}
		return sb.toString();
	}
}
