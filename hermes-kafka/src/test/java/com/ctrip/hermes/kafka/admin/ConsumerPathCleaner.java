package com.ctrip.hermes.kafka.admin;

import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;

public class ConsumerPathCleaner {
	public static void main(String[] args) {
		String zkConnString = "";
		CuratorFramework client = null;
		try {
			client = CuratorFrameworkFactory.newClient(zkConnString, new ExponentialBackoffRetry(1000, 3));
			client.start();

			List<String> childrens = client.getChildren().forPath("/consumers");
			for (String path : childrens) {
				System.out.println(path);
				client.delete().deletingChildrenIfNeeded().forPath("/consumers/" + path);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			CloseableUtils.closeQuietly(client);
		}
	}
}
