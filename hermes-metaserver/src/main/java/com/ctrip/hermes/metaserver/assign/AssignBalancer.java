package com.ctrip.hermes.metaserver.assign;

import java.util.List;
import java.util.Map;

public interface AssignBalancer {
	public <K, V> Map<K, List<V>> assign(Map<K, List<V>> originAssigns, List<V> freeAssigns);
}
