package com.ctrip.hermes.consumer.engine.ack;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class AckHolderException extends Exception {
	private static final long serialVersionUID = -4309903428974069057L;

	public AckHolderException() {
		super();
	}

	public AckHolderException(String message) {
		super(message);
	}

	public AckHolderException(String message, Throwable cause) {
		super(message, cause);
	}

	public AckHolderException(Throwable cause) {
		super(cause);
	}

	protected AckHolderException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
