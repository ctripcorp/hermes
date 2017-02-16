package com.ctrip.hermes.core.exception;

import com.ctrip.hermes.core.message.ProducerMessage;

public class MessageSendException extends Exception {

	private static final long serialVersionUID = 1L;

	private ProducerMessage<?> m_rawMessage;

	public MessageSendException() {
		super();
	}

	public MessageSendException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace,
	      ProducerMessage<?> rawMessage) {
		super(message, cause, enableSuppression, writableStackTrace);
		m_rawMessage = rawMessage;
	}

	public MessageSendException(String message, Throwable cause, ProducerMessage<?> rawMessage) {
		super(message, cause);
		m_rawMessage = rawMessage;
	}

	public MessageSendException(String message, ProducerMessage<?> rawMessage) {
		super(message);
		m_rawMessage = rawMessage;
	}

	public MessageSendException(Throwable cause, ProducerMessage<?> rawMessage) {
		super(cause);
		m_rawMessage = rawMessage;
	}

	public ProducerMessage<?> getRawMessage() {
		return m_rawMessage;
	}

}
