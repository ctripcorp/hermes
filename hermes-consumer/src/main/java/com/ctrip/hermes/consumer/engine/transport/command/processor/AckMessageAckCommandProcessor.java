package com.ctrip.hermes.consumer.engine.transport.command.processor;

import java.util.Arrays;
import java.util.List;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.consumer.engine.monitor.AckMessageAcceptanceMonitor;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;
import com.ctrip.hermes.core.transport.command.v5.AckMessageAckCommandV5;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class AckMessageAckCommandProcessor implements CommandProcessor {

	@Inject
	private AckMessageAcceptanceMonitor m_acceptMonitor;

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.ACK_MESSAGE_ACK_V5);
	}

	@Override
	public void process(CommandProcessorContext ctx) {
		AckMessageAckCommandV5 cmd = (AckMessageAckCommandV5) ctx.getCommand();
		m_acceptMonitor.received(cmd);
	}

}
