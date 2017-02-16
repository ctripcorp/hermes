package com.ctrip.hermes.kafka.admin;

import java.util.List;

import org.I0Itec.zkclient.ZkClient;

public class ZKClientTest {

	public static void main(String[] args) {
		ZkClient zkClient = new ZkClient("");
		String basePath = "/consumers";
		for (String consumerId : zkClient.getChildren(basePath)) {
			String offsetPath = basePath + "/" + consumerId + "/offsets";
			List<String> zkTopics = zkClient.getChildren(offsetPath);
			System.out.println(zkTopics);
		}
	}
}
