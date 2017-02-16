package com.ctrip.hermes.consumer.engine.transport.command.processor;

import java.util.Arrays;
import java.util.List;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.consumer.engine.monitor.PullMessageAcceptanceMonitor;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;
import com.ctrip.hermes.core.transport.command.v5.PullMessageAckCommandV5;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class PullMessageAckCommandProcessor implements CommandProcessor {

	@Inject
	private PullMessageAcceptanceMonitor m_acceptMonitor;

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.ACK_MESSAGE_PULL_V5);
	}

	@Override
	public void process(CommandProcessorContext ctx) {
		PullMessageAckCommandV5 cmd = (PullMessageAckCommandV5) ctx.getCommand();
		m_acceptMonitor.received(cmd);
	}

}
