package com.ctrip.hermes.metaserver.event;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface Task {
	public void run() throws Exception;

	public String name();
}
