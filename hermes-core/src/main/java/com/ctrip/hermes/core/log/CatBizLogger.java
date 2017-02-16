package com.ctrip.hermes.core.log;

import java.util.HashMap;
import java.util.Map;

import org.unidal.lookup.annotation.Named;
import org.unidal.net.Networks;

import com.dianping.cat.Cat;

@Named
public class CatBizLogger implements BizLogger {

	private final static String m_localhost = Networks.forIp().getLocalHostAddress();

	public final static String scenario = "hermes";

	public final static String type = "biz";

	@Override
	public void log(BizEvent event) {
		Map<String, String> indexedTags = new HashMap<String, String>();
		indexedTags.put("type", type);
		indexedTags.put("host", m_localhost);
		indexedTags.put("eventType", event.getEventType());
		indexedTags.put("eventTime", String.valueOf(event.getEventTime()));
		for (Map.Entry<String, Object> entry : event.getDatas().entrySet()) {
			indexedTags.put(entry.getKey(), String.valueOf(entry.getValue()));
		}
		Map<String, String> storedTags = new HashMap<String, String>();
		Cat.logTags(scenario, indexedTags, storedTags);
	}

}
