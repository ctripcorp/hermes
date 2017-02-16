package com.ctrip.hermes.metaserver.fulltest;

import java.net.URL;
import java.net.URLClassLoader;

public class HermesClassLoader extends URLClassLoader {
	String[] customClasses;

	public HermesClassLoader(URL[] urls, String... customClasses) {
		super(urls);
		this.customClasses = customClasses;
	}

	@Override
	protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
		synchronized (getClassLoadingLock(name)) {
			// First, check if the class has already been loaded
			Class c = findLoadedClass(name);
			if (c == null) {
				/**
				 * if isCustomClass try to load by itself, else delegate to its parent.
				 */
				if (isCustomClass(name)) {
					try {
						c = findClass(name);
					} catch (Exception e) {
						// ignore
					}

					if (c == null) {
						try {
							if (getParent() != null) {
								c = getParent().loadClass(name);
							}
						} catch (ClassNotFoundException e) {
							// ClassNotFoundException thrown if class not found
							// from the non-null parent class loader
						}
					}
				} else {
					try {
						if (getParent() != null) {
							c = getParent().loadClass(name);
						}
					} catch (ClassNotFoundException e) {
						// ClassNotFoundException thrown if class not found
						// from the non-null parent class loader
					}

					if (c == null) {
						try {
							c = findClass(name);
						} catch (Exception e) {
							// ignore
						}
					}
				}

			}
			if (resolve) {
				resolveClass(c);
			}
			return c;
		}
	}

	private boolean isCustomClass(String name) {
		boolean isCustom = false;
		for (String customClass : customClasses) {
			if (name.startsWith(customClass)) {
				isCustom = true;
				break;
			}
		}
		return isCustom;
	}

}
