package com.ctrip.hermes.core.result;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface CompletionCallback<T> {
	void onSuccess(T result);

	void onFailure(Throwable t);
}
