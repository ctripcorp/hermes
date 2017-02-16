package com.ctrip.hermes.core.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class CollectionUtil {

	public static <T> T last(List<T> list) {
		return isNullOrEmpty(list) ? null : list.get(list.size() - 1);
	}

	public static <T> T first(List<T> list) {
		return isNullOrEmpty(list) ? null : list.get(0);
	}

	public static boolean isNullOrEmpty(Collection<?> collection) {
		return collection == null || collection.isEmpty();
	}

	public static boolean isNotEmpty(Collection<?> collection) {
		return collection != null && !collection.isEmpty();
	}

	public static boolean isNotEmpty(Map<?, ?> map) {
		return map != null && !map.isEmpty();
	}

	@SuppressWarnings("rawtypes")
	public static Collection collect(Collection inputCollection, Transformer transformer) {
		ArrayList answer = new ArrayList(inputCollection.size());
		collect(inputCollection, transformer, answer);
		return answer;
	}

	@SuppressWarnings("rawtypes")
	public static Collection collect(Iterator inputIterator, Transformer transformer) {
		ArrayList answer = new ArrayList();
		collect(inputIterator, transformer, answer);
		return answer;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static Collection collect(Iterator inputIterator, final Transformer transformer,
	      final Collection outputCollection) {
		if (inputIterator != null && transformer != null) {
			while (inputIterator.hasNext()) {
				Object item = inputIterator.next();
				Object value = transformer.transform(item);
				outputCollection.add(value);
			}
		}
		return outputCollection;
	}

	@SuppressWarnings("rawtypes")
	public static Collection collect(Collection inputCollection, final Transformer transformer,
	      final Collection outputCollection) {
		if (inputCollection != null) {
			return collect(inputCollection.iterator(), transformer, outputCollection);
		}
		return outputCollection;
	}

	public interface Transformer {

		public Object transform(Object input);

	}
}
