package com.ctrip.hermes.producer;

import org.unidal.initialization.AbstractModule;
import org.unidal.initialization.Module;
import org.unidal.initialization.ModuleContext;
import org.unidal.lookup.annotation.Named;

@Named(type = Module.class, value = HermesProducerModule.ID)
public class HermesProducerModule extends AbstractModule {
	public static final String ID = "hermes-producer";

	@Override
	public Module[] getDependencies(ModuleContext ctx) {
		return null;
	}

	@Override
	protected void execute(ModuleContext ctx) throws Exception {
	}

}
