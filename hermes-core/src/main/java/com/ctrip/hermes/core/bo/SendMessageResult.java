package com.ctrip.hermes.core.bo;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class SendMessageResult {
	private boolean m_success;

	private boolean m_shouldSkip;

	private String m_errorMessage;

	private transient boolean m_shouldResponse = true;

	public SendMessageResult(boolean success, boolean shouldSkip, String errorMessage) {
		this(success, shouldSkip, errorMessage, true);
	}

	public SendMessageResult(boolean success, boolean shouldSkip, String errorMessage, boolean shouldResponse) {
		super();
		m_success = success;
		m_shouldSkip = shouldSkip;
		m_errorMessage = errorMessage;
		m_shouldResponse = shouldResponse;
	}

	public boolean isSuccess() {
		return m_success;
	}

	public boolean isShouldSkip() {
		return m_shouldSkip;
	}

	public boolean isShouldResponse() {
		return m_shouldResponse;
	}

	public String getErrorMessage() {
		return m_errorMessage;
	}

	public void setSuccess(boolean success) {
		m_success = success;
	}

	public void setShouldSkip(boolean shouldSkip) {
		m_shouldSkip = shouldSkip;
	}

	public void setErrorMessage(String errorMessage) {
		m_errorMessage = errorMessage;
	}

	public void setShouldResponse(boolean shouldResponse) {
		m_shouldResponse = shouldResponse;
	}

}
