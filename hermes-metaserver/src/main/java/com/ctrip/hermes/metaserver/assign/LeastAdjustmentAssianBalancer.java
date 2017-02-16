package com.ctrip.hermes.metaserver.assign;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.unidal.lookup.annotation.Named;

@Named(type = AssignBalancer.class)
public class LeastAdjustmentAssianBalancer implements AssignBalancer {

	public <K, V> Map<K, List<V>> assign(Map<K, List<V>> originAssigns, List<V> freeAssigns) {
		if (originAssigns == null || originAssigns.isEmpty()) {
			return new HashMap<K, List<V>>();
		}

		LinkedList<V> linkedFreeAssigns = (freeAssigns == null) ? new LinkedList<V>() : new LinkedList<V>(freeAssigns);
		Collections.shuffle(linkedFreeAssigns);

		int total = linkedFreeAssigns.size();
		Map<K, List<V>> newAssigns = new HashMap<K, List<V>>();

		for (Map.Entry<K, List<V>> assign : originAssigns.entrySet()) {
			LinkedList<V> newAssignList = new LinkedList<V>();
			if (assign.getValue() == null) {
				newAssigns.put(assign.getKey(), newAssignList);
			} else {
				newAssignList.addAll(assign.getValue());
				total += assign.getValue().size();
			}
			Collections.shuffle(newAssignList);
			newAssigns.put(assign.getKey(), newAssignList);
		}

		int avg = total / originAssigns.size();
		int remainder = total % originAssigns.size();
		int countEqualsAvgPlus1 = 0;

		for (Map.Entry<K, List<V>> assign : newAssigns.entrySet()) {
			int originSize = assign.getValue().size();
			if (originSize > avg) {
				int targetSize = avg;
				if (countEqualsAvgPlus1 < remainder) {
					countEqualsAvgPlus1++;
					targetSize = avg + 1;
				}
				for (int i = 0; i < originSize - targetSize; i++) {
					linkedFreeAssigns.add(assign.getValue().remove(0));
				}
			}
		}

		if (!linkedFreeAssigns.isEmpty()) {
			for (Map.Entry<K, List<V>> assign : newAssigns.entrySet()) {
				int originSize = assign.getValue().size();
				if (originSize < avg + 1) {
					int targetSize = avg;
					if (countEqualsAvgPlus1 < remainder) {
						targetSize = avg + 1;
						countEqualsAvgPlus1++;
					}
					for (int i = 0; i < targetSize - originSize; i++) {
						assign.getValue().add(linkedFreeAssigns.removeFirst());
					}

					if (linkedFreeAssigns.isEmpty()) {
						break;
					}
				}
			}
		}

		return newAssigns;
	}
}
