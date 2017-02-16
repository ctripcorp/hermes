package com.ctrip.hermes.env.provider;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.env.ManualConfigProvider;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = ManualConfigProvider.class)
public class DefaultManualConfigProvider implements ManualConfigProvider,
		Initializable {

	@Override
	public boolean isManualConfigureModeOn() {
		return false;
	}

	@Override
	public String fetchManualConfig() {
		return null;
	}

	@Override
	public String getBrokers() {
		return "[]";
	}

	@Override
	public void initialize() throws InitializationException {
	}

}
