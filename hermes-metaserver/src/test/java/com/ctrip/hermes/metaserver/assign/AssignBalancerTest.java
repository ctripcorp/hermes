package com.ctrip.hermes.metaserver.assign;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.junit.Test;

public class AssignBalancerTest {

	@Test
	public void testAssign() {
		AssignBalancer a = new LeastAdjustmentAssianBalancer();
		List<Integer> freeAssigns = new LinkedList<>();
		
		long start = System.currentTimeMillis();
		for (int i = 21; i < 10000; i++) {
			freeAssigns.add(i);
		}
		System.out.println("Cost time: " + (System.currentTimeMillis() - start));
		
		// freeAssigns = Arrays.asList(21, 22, 23);
		Map<String, List<Integer>> originAssigns = new HashMap<>();
		originAssigns.put("a", Arrays.asList(0));
		originAssigns.put("b", Arrays.asList(1, 2));
		originAssigns.put("c", Arrays.asList(3, 4, 5));
		originAssigns.put("d", Arrays.asList(6, 7, 8, 9));
		originAssigns.put("e", Arrays.asList(10, 11, 12, 13, 14, 15));
		System.out.println(a.assign(originAssigns, freeAssigns));

	}
}
