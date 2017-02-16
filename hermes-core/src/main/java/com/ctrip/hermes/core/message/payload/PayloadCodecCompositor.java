package com.ctrip.hermes.core.message.payload;

import java.util.ArrayList;

public class PayloadCodecCompositor implements PayloadCodec {

	private ArrayList<PayloadCodec> m_codecs = new ArrayList<>();

	private String m_type;

	public PayloadCodecCompositor(String type) {
		super();
		m_type = type;
	}

	@Override
	public String getType() {
		return m_type;
	}

	@Override
	public byte[] encode(String topic, Object obj) {
		Object input = obj;

		for (PayloadCodec codec : m_codecs) {
			input = codec.encode(topic, input);
		}

		return (byte[]) input;
	}

	@Override
	public <T> T decode(byte[] raw, Class<T> clazz) {
		byte[] input = raw;
		T result = null;

		for (int i = m_codecs.size() - 1; i >= 0; i--) {
			if (i == 0) {
				result = m_codecs.get(i).decode(input, clazz);
			} else {
				input = m_codecs.get(i).decode(input, byte[].class);
			}
		}

		return result;
	}

	public void addPayloadCodec(PayloadCodec codec) {
		m_codecs.add(codec);
	}

}
