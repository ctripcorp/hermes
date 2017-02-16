/**
 * 
 */
package com.ctrip.hermes.broker.selector;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.unidal.tuple.Quad;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

/**
 * @author marsqing
 *
 *         Jun 28, 2016 10:53:41 AM
 */
@SuppressWarnings("rawtypes")
public class BizLogAnalyzer {

	static class Times {
		long bornTime = 0;
		long produceCmdRcv = 0;
		long transform = 0;
		long save = 0;
		long deliver = 0;
		long bizStart = 0;
	}

	static Map<String, Times> refKey2Times = new HashMap<>();

	// Topic, Partition, Priority, MsgId
	static Map<Quad<String, String, String, String>, Times> msgId2Times = new HashMap<>();

	public static void main(String[] args) throws Exception {

		File biz = new File("/opt/logs/hermes-broker/biz.log");

		List<String> lines = Files.readLines(biz, Charsets.UTF_8);

		System.out.println(String.format("%10s\t%s\t%s\t%s\t%s\t%s", "Total", "BornToRcv", "RcvToSave", "SaveToDeliver", "DeliverToBizStart", "MsgId"));

		for (String line : lines) {
			if (line == null || line.isEmpty()) {
				continue;
			}
			Map map = JSON.parseObject(line, Map.class);
			String eventType = (String) map.get("eventType");
			switch (eventType) {
			case "Message.Received":
				Times times = new Times();
				refKey2Times.put((String) map.get("refKey"), times);
				times.bornTime = (long) map.get("bornTime");
				times.produceCmdRcv = (long) map.get("eventTime");

				break;

			case "RefKey.Transformed":
				times = findTimesByRefKey((String) map.get("refKey"));
				if (times != null) {
					msgId2Times.put(getTppg(map), times);
					times.transform = (long) map.get("eventTime");
				}
				break;

			case "Message.Saved":
				times = findTimesByRefKey((String) map.get("refKey"));
				if (times != null) {
					times.save = (long) map.get("eventTime");
				}
				break;

			case "Message.Delivered":
				times = msgId2Times.get(getTppg(map));
				if (times != null) {
					times.deliver = (long) map.get("eventTime");
				}
				break;

			case "Message.Acked":
				times = msgId2Times.get(getTppg(map));
				if (times != null) {
					times.bizStart = (long) map.get("BizStart");

					long l1 = times.produceCmdRcv - times.bornTime;
					long l2 = times.save - times.produceCmdRcv;
					long l3 = times.deliver - times.save;
					long l4 = times.bizStart - times.deliver;
					long total = times.bizStart - times.bornTime;
					if (total > 0) {
						System.out.println(String.format("%10s\t%10s\t%10s\t%10s\t%10s\t%10s", total, l1, l2, l3, l4, map.get("msgId")));
					}
				}

				break;

			default:
				System.out.println("ERROR");
				break;
			}
		}
	}

	private static Quad<String, String, String, String> getTppg(Map map) {
		return new Quad<>(map.get("topic").toString(), map.get("partition").toString(), map.get("priority").toString(), (String) map.get("msgId").toString());
	}

	private static Times findTimesByRefKey(String refKey) {
		return refKey2Times.get(refKey);
	}

}
