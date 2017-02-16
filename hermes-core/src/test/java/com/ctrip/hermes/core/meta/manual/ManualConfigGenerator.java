package com.ctrip.hermes.core.meta.manual;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.unidal.helper.Files.IO;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.meta.manual.ManualConfig.ManualBrokerAssignemnt;
import com.ctrip.hermes.core.meta.manual.ManualConfig.ManualConsumerAssignemnt;
import com.ctrip.hermes.meta.entity.Meta;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class ManualConfigGenerator {
	public static void main(String[] args) throws Exception {
		ManualConfig config = new ManualConfig(System.currentTimeMillis(), createMeta(), createConsumerAssignments(),
		      createBrokerAssignments());

		System.out.println(JSON.toJSONString(config));
		System.out.println(JSON.toJSONString(config).getBytes().length);
	}

	private static Set<ManualBrokerAssignemnt> createBrokerAssignments() {
		Set<ManualBrokerAssignemnt> assignments = new HashSet<>();

		Map<String, List<Integer>> ownedTopicPartitions = new HashMap<>();
		ownedTopicPartitions.put("order_new", Arrays.asList(new Integer[] { 0, 1 }));
		ownedTopicPartitions.put("hello_world", Arrays.asList(new Integer[] { 0 }));
		ManualBrokerAssignemnt assign = new ManualBrokerAssignemnt("4376", ownedTopicPartitions);
		assignments.add(assign);

		return assignments;
	}

	private static Set<ManualConsumerAssignemnt> createConsumerAssignments() {
		Set<ManualConsumerAssignemnt> assignments = new HashSet<>();

		Map<String, List<Integer>> cAssignment = new HashMap<>();
		cAssignment.put("onebox", Arrays.asList(new Integer[] { 0, 1 }));
		ManualConsumerAssignemnt assign = new ManualConsumerAssignemnt("order_new", "leo1", cAssignment);
		assignments.add(assign);

		return assignments;
	}

	private static Meta createMeta() throws IOException {
		byte[] bytes = IO.INSTANCE.readFrom(ManualConfigGenerator.class.getResourceAsStream("manual-meta.xml"));
		return JSON.parseObject(bytes, Meta.class);
	}
}
