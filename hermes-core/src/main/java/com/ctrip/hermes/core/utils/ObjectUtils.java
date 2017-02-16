package com.ctrip.hermes.core.utils;

import com.alibaba.fastjson.JSON;

public class ObjectUtils {
	public static <T> T deepCopy(T obj, Class<T> clazz) {
		return (T) JSON.parseObject(JSON.toJSONString(obj), clazz);
	}
}
