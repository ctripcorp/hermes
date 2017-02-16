package com.ctrip.hermes.core.message.codec;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.message.payload.DeflaterPayloadCodec;
import com.ctrip.hermes.core.message.payload.GZipPayloadCodec;
import com.ctrip.hermes.core.message.payload.PayloadCodec;
import com.ctrip.hermes.core.message.payload.PayloadCodecFactory.CodecDesc;
import com.ctrip.hermes.core.transport.netty.Magic;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;
import com.ctrip.hermes.meta.entity.Codec;
import com.google.common.base.Charsets;

public class MessageCodecUtils {

	private static Map<String, PayloadCodec> compressionCodecs = new HashMap<>();

	/**
	 * 
	 * @param consumerMsg
	 * @return
	 */
	public static ByteBuffer getPayload(ByteBuffer consumerMsg) {
		Pair<ByteBuffer, Date> pair = getPayloadAndBornTime(consumerMsg);
		return pair != null ? pair.getKey() : null;
	}

	public static Pair<ByteBuffer, Date> getPayloadAndBornTime(ByteBuffer consumerMsg) {
		Magic.readAndCheckMagic(consumerMsg);// skip magic number
		consumerMsg.get();// skip version

		consumerMsg.getInt();// skip total length
		int headerLen = consumerMsg.getInt(); // header length
		consumerMsg.getInt(); // skip body length

		String codecType = getCodecType(consumerMsg);

		final int CRC_LENGTH = 8;
		consumerMsg.limit(consumerMsg.limit() - CRC_LENGTH);

		Date bornTime = getBornTime(consumerMsg);

		consumerMsg.position(consumerMsg.position() + headerLen);

		byte[] rawBytes = new byte[consumerMsg.remaining()];
		consumerMsg.get(rawBytes);

		byte[] bytes;
		PayloadCodec compressionCodec = getCompressionCodec(codecType);
		if (compressionCodec != null) {
			bytes = compressionCodec.decode(rawBytes, byte[].class);
		} else {
			bytes = rawBytes;
		}

		return new Pair<ByteBuffer, Date>(ByteBuffer.wrap(bytes), bornTime);
	}

	private static PayloadCodec getCompressionCodec(String codecType) {
		if (!compressionCodecs.containsKey(codecType)) {
			synchronized (MessageCodecUtils.class) {
				if (!compressionCodecs.containsKey(codecType)) {
					CodecDesc codecDesc = CodecDesc.valueOf(codecType);
					if (Codec.GZIP.equals(codecDesc.getCompressionAlgo())) {
						compressionCodecs.put(codecType, new GZipPayloadCodec());
					} else if (Codec.DEFLATER.equals(codecDesc.getCompressionAlgo())) {
						DeflaterPayloadCodec codec = new DeflaterPayloadCodec();
						codec.setLevel(codecDesc.getLevel());
						compressionCodecs.put(codecType, codec);
					} else {
						compressionCodecs.put(codecType, new DummyPayloadCodec());
					}
				}
			}
		}

		return compressionCodecs.get(codecType);
	}

	private static class DummyPayloadCodec implements PayloadCodec {

		@Override
		public String getType() {
			return "dummy";
		}

		@Override
		public byte[] encode(String topic, Object obj) {
			throw new UnsupportedOperationException();
		}

		@SuppressWarnings("unchecked")
		@Override
		public <T> T decode(byte[] raw, Class<T> clazz) {
			return (T) raw;
		}

	}

	private static Date getBornTime(ByteBuffer consumerMsg) {
		consumerMsg.mark();
		byte firstByte = consumerMsg.get();
		if (HermesPrimitiveCodec.NULL != firstByte) {
			consumerMsg.position(consumerMsg.position() - 1);
			int refKeyLen = consumerMsg.getInt();
			consumerMsg.position(consumerMsg.position() + refKeyLen);
		}

		Date bornTime = new Date(consumerMsg.getLong());
		consumerMsg.reset();

		return bornTime;
	}

	private static String getCodecType(ByteBuffer consumerMsg) {
		consumerMsg.mark();
		String codecType = Codec.AVRO;
		byte firstByte = consumerMsg.get();// skip refKey
		if (firstByte != HermesPrimitiveCodec.NULL) {
			consumerMsg.position(consumerMsg.position() - 1);
			int length = consumerMsg.getInt();
			consumerMsg.position(consumerMsg.position() + length);
		}

		consumerMsg.getLong();// skip bornTime
		consumerMsg.getInt();// skip remaining retries

		firstByte = consumerMsg.get();
		if (firstByte != HermesPrimitiveCodec.NULL) {
			consumerMsg.position(consumerMsg.position() - 1);
			int length = consumerMsg.getInt();
			byte[] codecTypeBytes = new byte[length];
			consumerMsg.get(codecTypeBytes);
			codecType = new String(codecTypeBytes, Charsets.UTF_8);
		}

		consumerMsg.reset();
		return codecType;
	}

	/**
	 * 
	 * @param consumerMsg
	 * @return
	 */
	public static byte[] getPayload(byte[] consumerMsg) {
		ByteBuffer byteBuffer = ByteBuffer.wrap(consumerMsg);
		return getPayload(byteBuffer).array();
	}
}
