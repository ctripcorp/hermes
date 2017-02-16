package com.ctrip.hermes.broker.perf;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Charsets;

public class JsonPerfTest {
	public static void main(String[] args) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < 1000; i++) {
			sb.append("x");
		}
		String str = sb.toString();

		JSON.toJSONString(str);

		long start = System.currentTimeMillis();
		for (int i = 0; i < 10000; i++) {
			// JSON.toJSONString(str);
			 JSON.toJSONBytes(str);
			str.getBytes(Charsets.UTF_8);
		}
		System.out.println(System.currentTimeMillis() - start);
	}
}
