package com.ctrip.hermes.metaserver.commons;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.alibaba.fastjson.serializer.ValueFilter;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Property;
import com.ctrip.hermes.meta.entity.Storage;

public class MetaUtils {
	private static final SerializeConfig JSON_CFG = new SerializeConfig();

	public static String filterSensitiveField(Meta meta) {
		final Storage storage = meta.findStorage(Storage.MYSQL);

		return JSON.toJSONString(meta, JSON_CFG, new ValueFilter() {
			@Override
			@SuppressWarnings("unchecked")
			public Object process(Object owner, String fieldName, Object fieldValue) {
				if (storage != null && owner instanceof Datasource && fieldValue instanceof Map) {
					for (Datasource d : storage.getDatasources()) {
						if (d.getId().equals(((Datasource) owner).getId())) {
							Map<String, Property> newFieldValue = new HashMap<String, Property>();
							for (Entry<String, Property> entry : ((Map<String, Property>) fieldValue).entrySet()) {
								newFieldValue.put(entry.getKey(), new Property(entry.getKey()).setValue("**********"));
							}
							return newFieldValue;
						}
					}
				}
				return fieldValue;
			}
		});
	}
}
