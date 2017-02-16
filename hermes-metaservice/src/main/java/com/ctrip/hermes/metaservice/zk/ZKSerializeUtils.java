package com.ctrip.hermes.metaservice.zk;

import java.lang.reflect.Type;

import com.alibaba.fastjson.JSON;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class ZKSerializeUtils {
	public static byte[] serialize(Object obj) {
		return JSON.toJSONBytes(obj);
	}

	public static <T> T deserialize(byte[] bytes, Class<T> clazz) {
		if (bytes != null && bytes.length > 0) {
			return JSON.parseObject(bytes, clazz);
		} else {
			return null;
		}
	}

	public static <T> T deserialize(byte[] bytes, Type type) {
		if (bytes != null && bytes.length > 0) {
			return JSON.parseObject(bytes, type);
		} else {
			return null;
		}
	}

}
