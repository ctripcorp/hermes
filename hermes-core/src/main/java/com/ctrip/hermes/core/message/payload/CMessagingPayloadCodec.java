package com.ctrip.hermes.core.message.payload;

import org.unidal.lookup.annotation.Named;

import com.google.common.base.Charsets;

@Named(type = PayloadCodec.class, value = com.ctrip.hermes.meta.entity.Codec.CMESSAGING)
public class CMessagingPayloadCodec extends AbstractPayloadCodec {

	@Override
	public byte[] doEncode(String topic, Object obj) {
		if (obj instanceof String) {
			return ((String) obj).getBytes(Charsets.UTF_8);
		} else {
			throw new IllegalArgumentException("CMessaging producer messages should be String type, illegal message type "
			      + obj.getClass());
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T doDecode(byte[] raw, Class<T> clazz) {
		if (clazz == byte[].class) {
			return (T) raw;
		} else {
			throw new IllegalArgumentException("CMessaging consumer messages should be byte[] type, illegal message type "
			      + clazz);
		}
	}

	@Override
	public String getType() {
		return com.ctrip.hermes.meta.entity.Codec.CMESSAGING;
	}

}
