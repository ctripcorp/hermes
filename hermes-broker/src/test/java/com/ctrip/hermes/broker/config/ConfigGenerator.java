/**
 * 
 */
package com.ctrip.hermes.broker.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

/**
 * @author marsqing
 *
 *         Jul 6, 2016 11:05:19 AM
 */
public class ConfigGenerator {

	@Test
	public void test() {
		List<String> getters = Arrays.asList(//
				"getPullMessageSelectorWriteOffsetTtlMillis", //
				"getPullMessageSelectorSafeTriggerIntervalMillis", //
				"getPullMessageSelectorOffsetLoaderThreadPoolSize", //
				"getPullMessageSelectorOffsetLoaderThreadPoolKeepaliveSeconds", //
				"getPullMessageSelectorSafeTriggerMinFireIntervalMillis", //
				"getSendMessageSelectorSafeTriggerMinFireIntervalMillis", //
				"getSendMessageSelectorSafeTriggerIntervalMillis"//
		);

		StringBuilder init = new StringBuilder();
		StringBuilder fields = new StringBuilder();
		for (String g : getters) {
			List<String> parts = new ArrayList<>();

			// remove get
			g = g.substring(3);

			int lastUpper = 0;
			for (int i = 1; i < g.length(); i++) {
				if (Character.isUpperCase(g.charAt(i))) {
					parts.add(g.substring(lastUpper, i));
					lastUpper = i;
				}
				if (i == g.length() - 1) {
					parts.add(g.substring(lastUpper, i + 1));
				}
			}

			String fmt = "String %1$s = m_env.getGlobalConfig().getProperty(\"%2$s\");\n"//
					+ "if (StringUtils.isNumeric(%1$s)) {\n" //
					+ "\tm_%1$s = Integer.valueOf(%1$s);\n"//
					+ "}\n";

			init.append(String.format(fmt, toCamal(parts), toDotSeparated("broker.", parts)));
			fields.append("private int m_" + toCamal(parts) + ";\n");
			System.out.println("return m_" + toCamal(parts) + ";");
		}
		System.out.println(fields);
		System.out.println(init);
	}

	private String toDotSeparated(String prefix, List<String> parts) {
		StringBuilder sb = new StringBuilder();
		sb.append(prefix);

		for (int i = 0; i < parts.size(); i++) {
			sb.append(parts.get(i).toLowerCase());
			if (i != parts.size() - 1) {
				sb.append(".");
			}
		}

		return sb.toString();
	}

	private String toCamal(List<String> parts) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < parts.size(); i++) {
			if (i == 0) {
				sb.append(parts.get(i).toLowerCase());
			} else {
				sb.append(parts.get(i));
			}
		}
		return sb.toString();
	}

}
