package com.ctrip.hermes.broker.queue.storage.filter;

import java.util.Map;

public interface Filter {
	public boolean isMatch(String tag, String filter, Map<String, String> source);
}
