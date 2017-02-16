package com.ctrip.hermes.core.message.payload;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.unidal.helper.Files.AutoClose;
import org.unidal.helper.Files.IO;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.meta.entity.Codec;

@Named(type = PayloadCodec.class, value = com.ctrip.hermes.meta.entity.Codec.GZIP)
public class GZipPayloadCodec extends AbstractPayloadCodec {

	@Override
	public String getType() {
		// not top level codec, won't be called
		return Codec.GZIP;
	}

	@Override
	public byte[] doEncode(String topic, Object obj) {
		if (!(obj instanceof byte[])) {
			throw new IllegalArgumentException(String.format("Can not encode input of type %s",
			      obj == null ? "null" : obj.getClass()));
		}

		ByteArrayInputStream input = new ByteArrayInputStream((byte[]) obj);
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		try {
			GZIPOutputStream gout = new GZIPOutputStream(bout);
			IO.INSTANCE.copy(input, gout, AutoClose.INPUT_OUTPUT);
		} catch (IOException e) {
			throw new RuntimeException(String.format("Unexpected exception when encode %s of topic %s", obj, topic), e);
		}

		return bout.toByteArray();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T doDecode(byte[] input, Class<T> clazz) {
		GZIPInputStream gin = null;
		try {
			gin = new GZIPInputStream(new ByteArrayInputStream(input));
			ByteArrayOutputStream bout = new ByteArrayOutputStream();
			IO.INSTANCE.copy(gin, bout);
			return (T) bout.toByteArray();
		} catch (IOException e) {
			throw new RuntimeException(String.format("Unexpected exception when decoding"), e);
		} finally {
			if (gin != null) {
				try {
					gin.close();
				} catch (IOException e) {
					throw new RuntimeException(String.format("Failed to close GZIPInputStream!"), e);
				}
			}
		}
	}

}
