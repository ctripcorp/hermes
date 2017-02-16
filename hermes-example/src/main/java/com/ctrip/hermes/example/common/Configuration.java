package com.ctrip.hermes.example.common;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.util.StringUtils;

import com.google.common.collect.ImmutableMap;

public class Configuration {
	private static final Logger LOGGER = LoggerFactory.getLogger(Configuration.class);

	private static List<String> defaultResources = new CopyOnWriteArrayList();
	private static ClassLoader classLoader;
	private static Map<String, String> configMap = new HashMap();

	private static ArrayList<String> configFiles = new ArrayList();

	public static Map<String, String> getAllConfig() {
		return ImmutableMap.copyOf(configMap);
	}

	public static void addResource(String name) {
		loadConfig(name);
	}

	public static synchronized void addDefaultResource(String name) {
		if (!defaultResources.contains(name))
			defaultResources.add(name);
	}

	private static void loadDefaultConfig() {
		for (String defaultResource : defaultResources)
			loadConfig(defaultResource);
	}

	private static void loadConfig(String confFile) {
		Properties props = new Properties();
		InputStream in = null;
		try {
			URL url = classLoader.getResource(confFile);
			if (url == null) {
				return;
			}
			in = url.openStream();
			props.load(in);
			Enumeration en = props.propertyNames();
			while (en.hasMoreElements()) {
				String key = (String) en.nextElement();
				configMap.put(key, props.getProperty(key));
			}

			configFiles.add(confFile);
		} catch (Exception e) {
			LOGGER.warn("Cannot load configuration from file <" + confFile + ">", e);
		} finally {
			if (in != null)
				try {
					in.close();
				} catch (IOException ignored) {
				}
		}
	}

	public static String get(String name) {
		return getTrimmed(name);
	}

	public static String get(String name, String defaultValue) {
		String result = get(name);
		if (result == null) {
			result = defaultValue;
		}
		return result;
	}

	private static String getTrimmed(String name) {
		String value = (String) configMap.get(name);
		if (null == value) {
			return null;
		}
		return value.trim();
	}

	public static int getInt(String name, int defaultValue) {
		String valueString = get(name);
		if (valueString == null) {
			return defaultValue;
		}
		return Integer.parseInt(valueString);
	}

	public static int getInt(String name) {
		return Integer.parseInt(get(name));
	}

	public static boolean getBoolean(String name, boolean defaultValue) {
		String valueString = get(name);
		if (valueString == null) {
			return defaultValue;
		}
		return Boolean.valueOf(valueString).booleanValue();
	}

	public static boolean getBoolean(String name) {
		return Boolean.valueOf(get(name)).booleanValue();
	}

	public static String[] getStrings(String name) {
		String valueString = get(name);
		return getTrimmedStrings(valueString);
	}

	private static String[] getTrimmedStrings(String str) {
		String[] emptyStringArray = new String[0];
		if ((null == str) || ("".equals(str.trim()))) {
			return emptyStringArray;
		}

		return str.trim().split("\\s*,\\s*");
	}

	public static Class<?> getClass(String name) throws ClassNotFoundException {
		String valueString = getTrimmed(name);
		if (valueString == null) {
			throw new ClassNotFoundException("Class " + name + " not found");
		}
		return Class.forName(valueString, true, classLoader);
	}

	public static Class<?>[] getClasses(String name) throws ClassNotFoundException {
		String[] classNames = getStrings(name);
		if (classNames == null) {
			return null;
		}
		Class[] classes = new Class[classNames.length];
		for (int i = 0; i < classNames.length; i++) {
			classes[i] = getClass(classNames[i]);
		}
		return classes;
	}

	public static void dumpDeprecatedKeys() {
		for (String key : configMap.keySet())
			System.out.println(key + "=" + (String) configMap.get(key));
	}

	public static void set(String key, String value) {
		if (StringUtils.isEmpty(key)) {
			throw new IllegalArgumentException("Key [" + key + "] is blank, invalid");
		}
		if (StringUtils.isEmpty(value)) {
			throw new IllegalArgumentException("Value [" + value + "] is blank, invalid");
		}
		configMap.put(key, value);
	}

	public static void set(String key, String value, boolean isWriterToFile) {
		if (isWriterToFile) {
			String fileToWrite = null;

			if (configFiles.size() == 1) {
				fileToWrite = (String) configFiles.get(0);
			} else {
				for (String config : configFiles) {
					if (!config.contains("default")) {
						fileToWrite = config;
					}
				}
			}
			InputStream in = null;
			Properties props = new Properties();
			try {
				URL url = classLoader.getResource(fileToWrite);

				in = url.openStream();
				props.load(in);
				props.put(key, value);
				props.store(new FileOutputStream(new File(url.toURI())), null);
				in.close();
			} catch (IOException ioe) {
				LOGGER.error("Cannot Write configuration TO <" + fileToWrite + ">", ioe);
			} catch (URISyntaxException e) {
				LOGGER.error("Cannot Write configuration TO <" + fileToWrite + ">: URISyntaxException", e);
			}
		}
		set(key, value);
	}

	public static String getPropOrConfig(String key, String defaultValue) {
		String value = System.getProperty(key);
		if (StringUtils.isEmpty(value)) {
			value = get(key);
			if (StringUtils.isEmpty(value)) {
				return defaultValue;
			}
		}
		return value;
	}

	static {
		classLoader = Thread.currentThread().getContextClassLoader();
		if (classLoader == null) {
			classLoader = Configuration.class.getClassLoader();
		}
		loadDefaultConfig();
	}
}
