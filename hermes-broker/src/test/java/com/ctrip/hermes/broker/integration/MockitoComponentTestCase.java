package com.ctrip.hermes.broker.integration;

import java.util.HashMap;
import java.util.Map;

import org.codehaus.plexus.PlexusConstants;
import org.codehaus.plexus.PlexusContainer;
import org.codehaus.plexus.classworlds.realm.ClassRealm;
import org.codehaus.plexus.component.factory.ComponentFactory;
import org.codehaus.plexus.component.factory.ComponentInstantiationException;
import org.codehaus.plexus.component.repository.ComponentDescriptor;
import org.junit.After;
import org.junit.Before;
import org.unidal.lookup.ComponentTestCase;
import org.unidal.tuple.Pair;

public abstract class MockitoComponentTestCase extends ComponentTestCase {

	public static final String FACTORY = "MockitoPlexusComponentFactory";

	@Before
	@Override
	public final void setUp() throws Exception {
		super.setUp();
		defineComponent(ComponentFactory.class, FACTORY, MockitoPlexusComponentFactory.class);
	}

	@After
	@Override
	public final void tearDown() throws Exception {
		MockitoPlexusComponentFactory.clearMockitoComponent();
		super.tearDown();
	}

	protected <T> void defineMockitoComponent(Class<T> role, T instance) throws Exception {
		defineMockitoComponent(role, null, instance);
	}

	protected <T> void defineMockitoComponent(Class<T> role, String roleHint, T instance) throws Exception {
		defineComponentWithFactory(role, roleHint);
		MockitoPlexusComponentFactory.offerMockitoComponent(role, roleHint, instance);
	}

	private <T> void defineComponentWithFactory(Class<T> role, String roleHint) throws Exception {
		super.defineComponent(role, roleHint, role);

		if (roleHint == null) {
			roleHint = PlexusConstants.PLEXUS_DEFAULT_HINT;
		}

		getContainer().getComponentDescriptor(role, role.getName(), roleHint).setComponentFactory(FACTORY);
	}

	public static class MockitoPlexusComponentFactory implements ComponentFactory {

		private static Map<Pair<Class<?>, String>, Object> instances = new HashMap<>();

		public static <T> void offerMockitoComponent(Class<T> role, String roleHint, T instance) {
			roleHint = roleHint == null ? PlexusConstants.PLEXUS_DEFAULT_HINT : roleHint;
			instances.put(new Pair<Class<?>, String>(role, roleHint), instance);
		}

		public static void clearMockitoComponent() {
			instances.clear();
		}

		@Override
		public String getId() {
			return FACTORY;
		}

		@SuppressWarnings("rawtypes")
		@Override
		public Object newInstance(ComponentDescriptor desc, ClassRealm classRealm, PlexusContainer container)
		      throws ComponentInstantiationException {
			return instances.get(new Pair<>(desc.getRoleClass(), desc.getRoleHint()));
		}

	}

}
