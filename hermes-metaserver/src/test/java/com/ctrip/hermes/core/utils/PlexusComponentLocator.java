package com.ctrip.hermes.core.utils;

import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.unidal.lookup.ContainerLoader;

/**
 * Eliminate cache, for test only
 * 
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class PlexusComponentLocator {
	public static <T> T lookup(Class<T> clazz) {
		try {
			return ContainerLoader.getDefaultContainer().lookup(clazz);
		} catch (ComponentLookupException e) {
			throw new IllegalArgumentException(String.format("Error: Unable to lookup component %s!", clazz.getName()), e);
		}
	}

	public static <T> T lookup(Class<T> clazz, String roleHint) {
		try {
			return ContainerLoader.getDefaultContainer().lookup(clazz, roleHint);
		} catch (ComponentLookupException e) {
			throw new IllegalArgumentException(String.format("Error: Unable to lookup component %s!", clazz.getName()), e);
		}
	}
}
