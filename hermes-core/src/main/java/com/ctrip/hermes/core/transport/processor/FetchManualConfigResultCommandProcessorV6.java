package com.ctrip.hermes.core.transport.processor;

import java.util.Arrays;
import java.util.List;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;
import com.ctrip.hermes.core.transport.command.v6.FetchManualConfigResultCommandV6;
import com.ctrip.hermes.core.transport.monitor.FetchManualConfigResultMonitor;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class FetchManualConfigResultCommandProcessorV6 implements CommandProcessor {

	@Inject
	private FetchManualConfigResultMonitor m_resultMonitor;

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.RESULT_FETCH_MANUAL_CONFIG_V6);
	}

	@Override
	public void process(CommandProcessorContext ctx) {
		FetchManualConfigResultCommandV6 cmd = (FetchManualConfigResultCommandV6) ctx.getCommand();
		m_resultMonitor.resultReceived(cmd);
	}

}
