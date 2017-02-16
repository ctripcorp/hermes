package com.ctrip.hermes.core.transport.command;

import io.netty.buffer.ByteBuf;

import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class PullMessageCommand extends AbstractCommand {

	private static final long serialVersionUID = 8392887545356755515L;

	private String m_groupId;

	private String m_topic;

	private int m_partition;

	private int m_size;

	private long m_expireTime;

	private transient SettableFuture<PullMessageResultCommand> m_future;

	public PullMessageCommand() {
		this(null, -1, null, 0, -1L);
	}

	public PullMessageCommand(String topic, int partition, String groupId, int size, long expireTime) {
		super(CommandType.MESSAGE_PULL, 1);
		m_topic = topic;
		m_partition = partition;
		m_groupId = groupId;
		m_size = size;
		m_expireTime = expireTime;
	}

	public SettableFuture<PullMessageResultCommand> getFuture() {
		return m_future;
	}

	public void setFuture(SettableFuture<PullMessageResultCommand> future) {
		m_future = future;
	}

	public long getExpireTime() {
		return m_expireTime;
	}

	public int getSize() {
		return m_size;
	}

	public String getGroupId() {
		return m_groupId;
	}

	public String getTopic() {
		return m_topic;
	}

	public int getPartition() {
		return m_partition;
	}

	public void onResultReceived(PullMessageResultCommand ack) {
		m_future.set(ack);
	}

	@Override
	public void parse0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);

		m_topic = codec.readString();
		m_partition = codec.readInt();
		m_groupId = codec.readString();
		m_size = codec.readInt();
		m_expireTime = codec.readLong();
	}

	@Override
	public void toBytes0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);

		codec.writeString(m_topic);
		codec.writeInt(m_partition);
		codec.writeString(m_groupId);
		codec.writeInt(m_size);
		codec.writeLong(m_expireTime);
	}

	@Override
	public String toString() {
		return "PullMessageCommand [m_groupId=" + m_groupId + ", m_topic=" + m_topic + ", m_partition=" + m_partition
		      + ", m_size=" + m_size + ", m_expireTime=" + m_expireTime + ", m_header=" + m_header + "]";
	}

}
