package com.ctrip.hermes.core.transport.command.processor;

import java.util.List;

import com.ctrip.hermes.core.transport.command.CommandType;

public interface CommandProcessor {

	public List<CommandType> commandTypes(); 
	
	public void process(CommandProcessorContext ctx);
	
}
