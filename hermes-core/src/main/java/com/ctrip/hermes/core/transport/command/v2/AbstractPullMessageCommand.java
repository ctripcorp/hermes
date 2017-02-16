package com.ctrip.hermes.core.transport.command.v2;

import com.ctrip.hermes.core.transport.command.AbstractCommand;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.PullMessageResultListener;
import com.ctrip.hermes.core.transport.command.v5.PullMessageResultCommandV5;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
abstract public class AbstractPullMessageCommand extends AbstractCommand implements PullMessageResultListener {

	private static final long serialVersionUID = -8015825851040624144L;

	protected String m_topic;

	protected int m_partition;

	protected long m_expireTime;

	private transient SettableFuture<PullMessageResultCommandV5> m_future;

	public AbstractPullMessageCommand(CommandType commandType, int commandVersion, String topic, int partition,
	      long expireTime) {
		super(commandType, commandVersion);
		m_topic = topic;
		m_partition = partition;
		m_expireTime = expireTime;
	}

	public SettableFuture<PullMessageResultCommandV5> getFuture() {
		return m_future;
	}

	public void setFuture(SettableFuture<PullMessageResultCommandV5> future) {
		m_future = future;
	}

	public long getExpireTime() {
		return m_expireTime;
	}

	public String getTopic() {
		return m_topic;
	}

	public int getPartition() {
		return m_partition;
	}

	public void onResultReceived(PullMessageResultCommandV5 ack) {
		getFuture().set(ack);
	}

}
