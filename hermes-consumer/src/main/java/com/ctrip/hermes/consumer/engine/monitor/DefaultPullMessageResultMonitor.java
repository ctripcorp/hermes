package com.ctrip.hermes.consumer.engine.monitor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.transport.command.PullMessageResultListener;
import com.ctrip.hermes.core.transport.command.v5.PullMessageResultCommandV5;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = PullMessageResultMonitor.class)
public class DefaultPullMessageResultMonitor implements PullMessageResultMonitor {
	private static final Logger log = LoggerFactory.getLogger(DefaultPullMessageResultMonitor.class);

	private Map<Long, PullMessageResultListener> m_cmds = new ConcurrentHashMap<Long, PullMessageResultListener>();

	@Override
	public void monitor(PullMessageResultListener pullMessageResultListener) {
		if (pullMessageResultListener != null) {
			m_cmds.put(pullMessageResultListener.getHeader().getCorrelationId(), pullMessageResultListener);
		}
	}

	@Override
	public void resultReceived(PullMessageResultCommandV5 result) {
		if (result != null) {
			PullMessageResultListener listener = m_cmds.remove(result.getHeader().getCorrelationId());

			if (listener != null) {
				try {
					listener.onResultReceived(result);
				} catch (Exception e) {
					log.warn("Exception occurred while calling resultReceived", e);
				}
			} else {
				result.release();
			}
		}
	}

	@Override
	public void remove(PullMessageResultListener pullMessageResultListener) {
		if (pullMessageResultListener != null) {
			m_cmds.remove(pullMessageResultListener.getHeader().getCorrelationId());
		}
	}
}
