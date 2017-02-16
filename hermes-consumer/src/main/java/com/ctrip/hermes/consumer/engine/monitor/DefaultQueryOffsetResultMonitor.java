package com.ctrip.hermes.consumer.engine.monitor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.transport.command.v5.QueryLatestConsumerOffsetCommandV5;
import com.ctrip.hermes.core.transport.command.v5.QueryOffsetResultCommandV5;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = QueryOffsetResultMonitor.class)
public class DefaultQueryOffsetResultMonitor implements QueryOffsetResultMonitor {
	private static final Logger log = LoggerFactory.getLogger(DefaultQueryOffsetResultMonitor.class);

	private Map<Long, QueryLatestConsumerOffsetCommandV5> m_cmds = new ConcurrentHashMap<Long, QueryLatestConsumerOffsetCommandV5>();

	@Override
	public void monitor(QueryLatestConsumerOffsetCommandV5 cmd) {
		if (cmd != null) {
			m_cmds.put(cmd.getHeader().getCorrelationId(), cmd);
		}
	}

	@Override
	public void resultReceived(QueryOffsetResultCommandV5 result) {
		if (result != null) {
			QueryLatestConsumerOffsetCommandV5 queryOffsetCommand = null;
			queryOffsetCommand = m_cmds.remove(result.getHeader().getCorrelationId());

			if (queryOffsetCommand != null) {
				try {
					queryOffsetCommand.onResultReceived(result);
				} catch (Exception e) {
					log.warn("Exception occurred while calling resultReceived", e);
				}
			}
		}
	}

	@Override
	public void remove(QueryLatestConsumerOffsetCommandV5 cmd) {
		if (cmd != null) {
			m_cmds.remove(cmd.getHeader().getCorrelationId());
		}
	}
}
