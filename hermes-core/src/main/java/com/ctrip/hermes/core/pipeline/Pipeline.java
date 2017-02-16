package com.ctrip.hermes.core.pipeline;

public interface Pipeline<T> {

	public T put(Object msg);
}
