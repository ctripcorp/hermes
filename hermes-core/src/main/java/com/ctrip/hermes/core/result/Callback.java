package com.ctrip.hermes.core.result;

public interface Callback {
	public void onCompletion(SendResult sendResult, Exception exception);
}
