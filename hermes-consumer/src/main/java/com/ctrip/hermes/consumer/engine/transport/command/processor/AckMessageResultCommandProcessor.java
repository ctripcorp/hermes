package com.ctrip.hermes.consumer.engine.transport.command.processor;

import java.util.Arrays;
import java.util.List;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.consumer.engine.monitor.AckMessageResultMonitor;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;
import com.ctrip.hermes.core.transport.command.v5.AckMessageResultCommandV5;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class AckMessageResultCommandProcessor implements CommandProcessor {

	@Inject
	private AckMessageResultMonitor m_resultMonitor;

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.RESULT_ACK_MESSAGE_V5);
	}

	@Override
	public void process(CommandProcessorContext ctx) {
		AckMessageResultCommandV5 cmd = (AckMessageResultCommandV5) ctx.getCommand();
		m_resultMonitor.received(cmd);
	}

}
