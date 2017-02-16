package com.ctrip.hermes.consumer.engine.transport.command.processor;

import java.util.Arrays;
import java.util.List;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.consumer.engine.monitor.QueryOffsetResultMonitor;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;
import com.ctrip.hermes.core.transport.command.v5.QueryOffsetResultCommandV5;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class QueryOffsetResultCommandProcessor implements CommandProcessor {

	@Inject
	private QueryOffsetResultMonitor m_queryOffsetResultMonitor;

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.RESULT_QUERY_OFFSET_V5);
	}

	@Override
	public void process(CommandProcessorContext ctx) {
		QueryOffsetResultCommandV5 cmd = (QueryOffsetResultCommandV5) ctx.getCommand();
		m_queryOffsetResultMonitor.resultReceived(cmd);
	}

}
