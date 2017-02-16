package com.ctrip.hermes.consumer.engine.transport.command.processor;

import java.util.Arrays;
import java.util.List;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.consumer.engine.monitor.PullMessageResultMonitor;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;
import com.ctrip.hermes.core.transport.command.v5.PullMessageResultCommandV5;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class PullMessageResultCommandProcessor implements CommandProcessor {

	@Inject
	private PullMessageResultMonitor m_messageResultMonitor;

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.RESULT_MESSAGE_PULL_V5);
	}

	@Override
	public void process(CommandProcessorContext ctx) {
		PullMessageResultCommandV5 cmd = (PullMessageResultCommandV5) ctx.getCommand();
		m_messageResultMonitor.resultReceived(cmd);
	}

}
