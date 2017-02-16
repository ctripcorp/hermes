package com.ctrip.hermes.core.transport.command.processor;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.transport.command.CommandType;

@Named(type = CommandProcessorRegistry.class)
public class DefaultCommandProcessorRegistry extends ContainerHolder implements Initializable, CommandProcessorRegistry {

	private Map<CommandType, CommandProcessor> m_processors = new ConcurrentHashMap<CommandType, CommandProcessor>();

	@Override
	public void registerProcessor(CommandType type, CommandProcessor processor) {
		if (m_processors.containsKey(type)) {
			throw new IllegalArgumentException(String.format("Command processor for type %s is already registered", type));
		}

		m_processors.put(type, processor);
	}

	@Override
	public CommandProcessor findProcessor(CommandType type) {
		return m_processors.get(type);
	}

	@Override
	public void initialize() throws InitializationException {
		List<CommandProcessor> processors = lookupList(CommandProcessor.class);

		for (CommandProcessor p : processors) {
			for (CommandType type : p.commandTypes()) {
				registerProcessor(type, p);
			}
		}
	}

	@Override
	public Set<CommandProcessor> listAllProcessors() {
		return new HashSet<CommandProcessor>(m_processors.values());
	}

}
