package com.ctrip.hermes.metaserver.monitor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.transport.command.QueryMessageOffsetByTimeCommand;
import com.ctrip.hermes.core.transport.command.QueryOffsetResultCommand;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = QueryOffsetResultMonitor.class)
public class DefaultQueryOffsetResultMonitor implements QueryOffsetResultMonitor {
	private static final Logger log = LoggerFactory.getLogger(DefaultQueryOffsetResultMonitor.class);

	private Map<Long, QueryMessageOffsetByTimeCommand> m_cmds = new ConcurrentHashMap<Long, QueryMessageOffsetByTimeCommand>();

	@Override
	public void monitor(QueryMessageOffsetByTimeCommand cmd) {
		if (cmd != null) {
			m_cmds.put(cmd.getHeader().getCorrelationId(), cmd);
		}
	}

	@Override
	public void resultReceived(QueryOffsetResultCommand result) {
		if (result != null) {
			QueryMessageOffsetByTimeCommand queryOffsetCommand = null;
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
	public void remove(QueryMessageOffsetByTimeCommand cmd) {
		if (cmd != null) {
			m_cmds.remove(cmd.getHeader().getCorrelationId());
		}
	}
}
