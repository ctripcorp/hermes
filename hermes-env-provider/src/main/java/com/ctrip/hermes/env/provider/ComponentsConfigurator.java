package com.ctrip.hermes.env.provider;

import java.util.ArrayList;
import java.util.List;

import org.unidal.lookup.configuration.AbstractResourceConfigurator;
import org.unidal.lookup.configuration.Component;

public class ComponentsConfigurator extends AbstractResourceConfigurator {

	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();

		all.add(A(DefaultClientEnvironment.class));
		all.add(A(DefaultEnvProvider.class));
		all.add(A(DefaultManualConfigProvider.class));
		all.add(A(DefaultBrokerConfigProvider.class));

		return all;
	}

	public static void main(String[] args) {
		generatePlexusComponentsXmlFile(new ComponentsConfigurator());
	}
}
