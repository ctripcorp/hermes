package com.ctrip.hermes.broker;

import org.unidal.initialization.AbstractModule;
import org.unidal.initialization.Module;
import org.unidal.initialization.ModuleContext;
import org.unidal.lookup.annotation.Named;

import com.dianping.cat.CatClientModule;

@Named(type = Module.class, value = HermesBrokerModule.ID)
public class HermesBrokerModule extends AbstractModule {
	public static final String ID = "hermes-broker";

	@Override
	public Module[] getDependencies(ModuleContext ctx) {
		return ctx.getModules(CatClientModule.ID);
	}

	@Override
	protected void execute(ModuleContext ctx) throws Exception {
	}

}
