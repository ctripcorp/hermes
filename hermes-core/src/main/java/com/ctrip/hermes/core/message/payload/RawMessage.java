package com.ctrip.hermes.core.message.payload;

public class RawMessage {

	private byte[] m_encodedMessage;

	public RawMessage(byte[] ecodedMessage) {
		m_encodedMessage = ecodedMessage;
	}

	public byte[] getEncodedMessage() {
		return m_encodedMessage;
	}

}
