package com.ctrip.hermes.metaserver.event;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Named;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = EventHandlerRegistry.class)
public class DefaultEventHandlerRegistry extends ContainerHolder implements EventHandlerRegistry, Initializable {

	private Map<EventType, List<EventHandler>> m_handlers = new HashMap<>();

	@Override
	public List<EventHandler> findHandler(EventType type) {
		return m_handlers.get(type);
	}

	@Override
	public void initialize() throws InitializationException {
		List<EventHandler> handlers = lookupList(EventHandler.class);

		for (EventHandler handler : handlers) {
			EventType eventType = handler.eventType();
			if (!m_handlers.containsKey(eventType)) {
				m_handlers.put(eventType, new ArrayList<EventHandler>());
			}
			m_handlers.get(eventType).add(handler);
		}
	}

	@Override
	public void forEachHandler(Function fun) {
		for (List<EventHandler> handlers : m_handlers.values()) {
			for (EventHandler handler : handlers) {
				fun.apply(handler);
			}
		}
	}

}
