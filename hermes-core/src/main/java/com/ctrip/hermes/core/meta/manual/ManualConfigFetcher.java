package com.ctrip.hermes.core.meta.manual;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface ManualConfigFetcher {
	public ManualConfig fetchConfig(ManualConfig oldConfig);
}
