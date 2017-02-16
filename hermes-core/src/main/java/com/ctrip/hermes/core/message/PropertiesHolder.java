package com.ctrip.hermes.core.message;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class PropertiesHolder {

	public final static String SYS = "SYS.";

	public final static String APP = "APP.";

	private Map<String, String> m_durableProperties = new HashMap<String, String>();

	private Map<String, String> m_volatileProperties = new HashMap<String, String>();

	private Set<String> m_rawDurableAppPropertyNames = new HashSet<String>();

	public PropertiesHolder() {
	}

	public PropertiesHolder(Map<String, String> durableProperties, Map<String, String> volatileProperties) {
		setDurableProperties(durableProperties);
		setVolatileProperties(volatileProperties);
	}

	public void addDurableAppProperty(String name, String value) {
		m_rawDurableAppPropertyNames.add(name);
		m_durableProperties.put(APP + name, value);
	}

	public void addDurableSysProperty(String name, String value) {
		m_durableProperties.put(SYS + name, value);
	}

	public String getDurableAppProperty(String name) {
		return m_durableProperties.get(APP + name);
	}

	public String getDurableSysProperty(String name) {
		return m_durableProperties.get(SYS + name);
	}

	public void addVolatileProperty(String name, String value) {
		m_volatileProperties.put(name, value);
	}

	public String getVolatileProperty(String name) {
		return m_volatileProperties.get(name);
	}

	public Map<String, String> getDurableProperties() {
		return m_durableProperties;
	}

	public void setDurableProperties(Map<String, String> durableProperties) {
		if (durableProperties != null) {
			m_durableProperties = durableProperties;

			for (String mergedKey : durableProperties.keySet()) {
				if (mergedKey.startsWith(APP)) {
					m_rawDurableAppPropertyNames.add(mergedKey.substring(APP.length()));
				}
			}
		}
	}

	public Map<String, String> getVolatileProperties() {
		return m_volatileProperties;
	}

	public void setVolatileProperties(Map<String, String> volatileProperties) {
		if (volatileProperties != null) {
			m_volatileProperties = volatileProperties;
		}
	}

	public Set<String> getRawDurableAppPropertyNames() {
		return m_rawDurableAppPropertyNames;
	}

}
