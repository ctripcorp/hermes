package com.ctrip.hermes.core.utils;

import java.io.Serializable;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;

import com.dianping.cat.Cat;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Plugin(name = "CatErrorAppender", elementType = Appender.ELEMENT_TYPE, category = "Core")
public class CatLog4j2ErrorAppender extends AbstractAppender {

	protected CatLog4j2ErrorAppender(String name, Filter filter, Layout<? extends Serializable> layout,
	      boolean ignoreExceptions) {
		super(name, filter, layout, ignoreExceptions);
	}

	@PluginFactory
	public static CatLog4j2ErrorAppender createAppender(@PluginAttribute("name") String name,
	      @PluginElement("Filter") final Filter filter, @PluginElement("Layout") Layout<? extends Serializable> layout,
	      @PluginAttribute("ignoreExceptions") boolean ignoreExceptions) {
		return new CatLog4j2ErrorAppender(name, filter, layout, ignoreExceptions);
	}

	@Override
	public void append(LogEvent event) {
		if (event.getLevel().isMoreSpecificThan(Level.ERROR)) {
			logError(event);
		}
	}

	private void logError(LogEvent event) {
		try {
			Throwable throwable = event.getThrown();

			if (throwable != null) {
				Object message = event.getMessage();

				if (message != null) {
					Cat.logError(String.valueOf(message), throwable);
				} else {
					Cat.logError(throwable);
				}
			}
		} catch (Throwable e) {
			// ignore
		}
	}

}
