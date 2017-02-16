package com.ctrip.hermes.consumer.engine.transport.command.processor;

import java.util.Arrays;
import java.util.List;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.consumer.engine.monitor.QueryOffsetAcceptanceMonitor;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;
import com.ctrip.hermes.core.transport.command.v5.QueryLatestConsumerOffsetAckCommandV5;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class QueryOffsetAckCommandProcessor implements CommandProcessor {

	@Inject
	private QueryOffsetAcceptanceMonitor m_acceptMonitor;

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.ACK_QUERY_LATEST_CONSUMER_OFFSET_V5);
	}

	@Override
	public void process(CommandProcessorContext ctx) {
		QueryLatestConsumerOffsetAckCommandV5 cmd = (QueryLatestConsumerOffsetAckCommandV5) ctx.getCommand();
		m_acceptMonitor.received(cmd);
	}

}
