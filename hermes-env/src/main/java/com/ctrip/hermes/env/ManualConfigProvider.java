package com.ctrip.hermes.env;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface ManualConfigProvider {
	public boolean isManualConfigureModeOn();

	public String fetchManualConfig();

	public String getBrokers();
}
