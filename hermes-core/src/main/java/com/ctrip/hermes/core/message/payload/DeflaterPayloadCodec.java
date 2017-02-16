package com.ctrip.hermes.core.message.payload;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import org.unidal.helper.Files.AutoClose;
import org.unidal.helper.Files.IO;

import com.ctrip.hermes.meta.entity.Codec;

public class DeflaterPayloadCodec extends AbstractPayloadCodec {

	private int m_level = 5;

	public void setLevel(int level) {
		m_level = level;
	}

	@Override
	public String getType() {
		// not top level codec, won't be called
		return Codec.DEFLATER;
	}

	@Override
	public byte[] doEncode(String topic, Object obj) {
		if (!(obj instanceof byte[])) {
			throw new IllegalArgumentException(String.format("Can not encode input of type %s",
			      obj == null ? "null" : obj.getClass()));
		}

		ByteArrayInputStream input = new ByteArrayInputStream((byte[]) obj);
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		// should be true to be compatible with C# DeflateStream
		boolean nowrap = true;
		Deflater def = new Deflater(m_level, nowrap);
		try {
			DeflaterOutputStream gout = new DeflaterOutputStream(bout, def);
			IO.INSTANCE.copy(input, gout, AutoClose.INPUT_OUTPUT);
			return bout.toByteArray();
		} catch (IOException e) {
			throw new RuntimeException(String.format("Unexpected exception when encode %s of topic %s", obj, topic), e);
		} finally {
			if (def != null) {
				def.end();
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T doDecode(byte[] input, Class<T> clazz) {
		// should be true to be compatible with C# DeflateStream
		boolean nowrap = true;
		Inflater inf = new Inflater(nowrap);
		try {
			InflaterInputStream gin = new InflaterInputStream(new ByteArrayInputStream(input), inf);
			ByteArrayOutputStream bout = new ByteArrayOutputStream();
			IO.INSTANCE.copy(gin, bout);
			return (T) bout.toByteArray();
		} catch (IOException e) {
			throw new RuntimeException(String.format("Unexpected exception when decoding"), e);
		} finally {
			if (inf != null) {
				inf.end();
			}
		}
	}

}
