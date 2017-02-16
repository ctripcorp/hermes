package com.ctrip.hermes.metaservice.converter;

import java.util.HashSet;
import java.util.Set;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;

import com.alibaba.fastjson.JSON;

public class MetaModelToEntityConverter {

	public static com.ctrip.hermes.meta.entity.Meta convert(com.ctrip.hermes.metaservice.model.Meta model) {
		com.ctrip.hermes.meta.entity.Meta entity = new com.ctrip.hermes.meta.entity.Meta();
		entity = JSON.parseObject(model.getValue(), com.ctrip.hermes.meta.entity.Meta.class);
		BeanUtils.copyProperties(model, entity, getNullPropertyNames(model));
		return entity;
	}

	static String[] getNullPropertyNames(Object source) {
		final BeanWrapper src = new BeanWrapperImpl(source);
		java.beans.PropertyDescriptor[] pds = src.getPropertyDescriptors();

		Set<String> emptyNames = new HashSet<String>();
		for (java.beans.PropertyDescriptor pd : pds) {
			Object srcValue = src.getPropertyValue(pd.getName());
			if (srcValue == null)
				emptyNames.add(pd.getName());
		}
		String[] result = new String[emptyNames.size()];
		return emptyNames.toArray(result);
	}
}
