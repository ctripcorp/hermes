package com.ctrip.hermes.core.utils;

import java.util.HashMap;
import java.util.Map;

import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.unidal.lookup.ContainerLoader;
import org.unidal.tuple.Pair;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class PlexusComponentLocator {

	private static Map<Class<?>, Object> m_componentsCache = new HashMap<Class<?>, Object>();

	private static Map<Pair<Class<?>, String>, Object> m_componentsWithRoleHintCache = new HashMap<Pair<Class<?>, String>, Object>();

	@SuppressWarnings("unchecked")
	public static <T> T lookup(Class<T> clazz) {
		Object component = m_componentsCache.get(clazz);

		if (component == null) {
			synchronized (m_componentsCache) {
				component = m_componentsCache.get(clazz);
				if (component == null) {
					try {
						component = ContainerLoader.getDefaultContainer().lookup(clazz);
						m_componentsCache.put(clazz, component);
					} catch (ComponentLookupException e) {
						throw new IllegalArgumentException(String.format("Error: Unable to lookup component %s!",
						      clazz.getName()), e);
					}
				}
			}
		}

		return (T) component;
	}

	@SuppressWarnings("unchecked")
	public static <T> T lookup(Class<T> clazz, String roleHint) {
		Pair<Class<?>, String> key = new Pair<Class<?>, String>(clazz, roleHint);

		Object component = m_componentsWithRoleHintCache.get(key);

		if (component == null) {
			synchronized (m_componentsWithRoleHintCache) {
				component = m_componentsWithRoleHintCache.get(key);
				if (component == null) {
					try {
						component = ContainerLoader.getDefaultContainer().lookup(clazz, roleHint);
						m_componentsWithRoleHintCache.put(key, component);
					} catch (ComponentLookupException e) {
						throw new IllegalArgumentException(String.format(
						      "Error: Unable to lookup component %s with roleHint %s!", clazz.getName(), roleHint), e);
					}
				}
			}
		}

		return (T) component;
	}

	public static <T> T lookupWithoutCache(Class<T> clazz, String roleHint) {
		try {
			return (T) ContainerLoader.getDefaultContainer().lookup(clazz, roleHint);
		} catch (ComponentLookupException e) {
			throw new IllegalArgumentException(String.format("Error: Unable to lookup component %s with roleHint %s!",
			      clazz.getName(), roleHint), e);
		}
	}
}
