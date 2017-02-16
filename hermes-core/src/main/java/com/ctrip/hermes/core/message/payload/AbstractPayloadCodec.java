package com.ctrip.hermes.core.message.payload;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Suppress encode/decode when input is RawMessage
 * 
 * @author marsqing
 *
 */
public abstract class AbstractPayloadCodec implements PayloadCodec {

	private static final Logger log = LoggerFactory.getLogger(AbstractPayloadCodec.class);

	@SuppressWarnings("unchecked")
	@Override
	public <T> T decode(byte[] raw, Class<T> clazz) {
		try {
			if (clazz == RawMessage.class) {
				return (T) new RawMessage(raw);
			} else {
				return doDecode(raw, clazz);
			}
		} catch (Exception e) {
			log.warn("Decode failed.", e);
			throw e;
		}
	}

	@Override
	public byte[] encode(String topic, Object obj) {
		try {
			if (obj instanceof RawMessage) {
				return ((RawMessage) obj).getEncodedMessage();
			} else {
				return doEncode(topic, obj);
			}
		} catch (Exception e) {
			log.warn("Encode failed.", e);
			throw e;
		}
	}

	protected abstract byte[] doEncode(String topic, Object obj);

	protected abstract <T> T doDecode(byte[] raw, Class<T> clazz);

}
