package com.ctrip.hermes.core.transport.command.processor;

import java.util.Set;

import com.ctrip.hermes.core.transport.command.CommandType;

public interface CommandProcessorRegistry {

	public void registerProcessor(CommandType type, CommandProcessor processor);

	public CommandProcessor findProcessor(CommandType type);

	public Set<CommandProcessor> listAllProcessors();

}
