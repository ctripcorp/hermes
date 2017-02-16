package com.ctrip.hermes.consumer.engine.bootstrap.strategy;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface ConsumerTask {

	public void start();

	public void close();
}
