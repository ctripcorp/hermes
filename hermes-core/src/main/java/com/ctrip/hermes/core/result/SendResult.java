package com.ctrip.hermes.core.result;

import com.ctrip.hermes.core.message.ProducerMessage;

public class SendResult {

	private ProducerMessage<?> m_message;

	public SendResult() {
	}

	public SendResult(ProducerMessage<?> message) {
		m_message = message;
	}

	public ProducerMessage<?> getMessage() {
		return m_message;
	}

}
