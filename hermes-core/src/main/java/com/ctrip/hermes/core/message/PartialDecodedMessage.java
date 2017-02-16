package com.ctrip.hermes.core.message;

import io.netty.buffer.ByteBuf;

public class PartialDecodedMessage {

	private String m_bodyCodecType;

	private ByteBuf m_body;

	private String m_key;

	private long m_bornTime;

	private int m_remainingRetries = 0;

	private ByteBuf m_durableProperties;

	private ByteBuf m_volatileProperties;

	private byte[] m_bodyBytes;

	public String getBodyCodecType() {
		return m_bodyCodecType;
	}

	public void setBodyCodecType(String bodyCodecType) {
		m_bodyCodecType = bodyCodecType;
	}

	public int getRemainingRetries() {
		return m_remainingRetries;
	}

	public void setRemainingRetries(int remainingRetries) {
		m_remainingRetries = remainingRetries;
	}

	public void setBody(ByteBuf body) {
		m_body = body;
	}

	public void setKey(String key) {
		m_key = key;
	}

	public void setBornTime(long bornTime) {
		m_bornTime = bornTime;
	}

	public void setDurableProperties(ByteBuf durableProperties) {
		m_durableProperties = durableProperties;
	}

	public void setVolatileProperties(ByteBuf volatileProperties) {
		m_volatileProperties = volatileProperties;
	}

	public ByteBuf getBody() {
		return m_body.duplicate();
	}

	public byte[] readBody() {
		if (m_bodyBytes == null) {
			synchronized (this) {
				if (m_bodyBytes == null) {
					m_bodyBytes = readByteBuf(m_body);
				}
			}
		}

		return m_bodyBytes;
	}

	public String getKey() {
		return m_key;
	}

	public long getBornTime() {
		return m_bornTime;
	}

	public ByteBuf getDurableProperties() {
		return m_durableProperties == null ? null : m_durableProperties.duplicate();
	}

	public ByteBuf getVolatileProperties() {
		return m_volatileProperties == null ? null : m_volatileProperties.duplicate();
	}

	public byte[] readDurableProperties() {
		return readByteBuf(m_durableProperties);
	}

	public byte[] readVolatileProperties() {
		return readByteBuf(m_volatileProperties);
	}

	private byte[] readByteBuf(ByteBuf buf) {
		if (buf == null) {
			return null;
		}

		byte[] bytes = new byte[buf.readableBytes()];
		buf.readBytes(bytes);
		return bytes;
	}

}
