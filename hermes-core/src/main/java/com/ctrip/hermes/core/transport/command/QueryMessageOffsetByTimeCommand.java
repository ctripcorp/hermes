package com.ctrip.hermes.core.transport.command;

import io.netty.buffer.ByteBuf;

import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;
import com.google.common.util.concurrent.SettableFuture;

public class QueryMessageOffsetByTimeCommand extends AbstractCommand {

	private static final long serialVersionUID = 6527058959298076437L;

	private String m_topic;

	private int m_partition;

	private long m_time;

	private transient SettableFuture<QueryOffsetResultCommand> m_future;

	public QueryMessageOffsetByTimeCommand() {
		this(null, -1, Long.MIN_VALUE);
	}

	public QueryMessageOffsetByTimeCommand(String topic, int partition, long time) {
		super(CommandType.QUERY_MESSAGE_OFFSET_BY_TIME, 1);
		m_topic = topic;
		m_partition = partition;
		m_time = time;
	}

	public String getTopic() {
		return m_topic;
	}

	public int getPartition() {
		return m_partition;
	}

	public long getTime() {
		return m_time;
	}

	public SettableFuture<QueryOffsetResultCommand> getFuture() {
		return m_future;
	}

	public void setFuture(SettableFuture<QueryOffsetResultCommand> future) {
		m_future = future;
	}

	public void onResultReceived(QueryOffsetResultCommand result) {
		m_future.set(result);
	}

	@Override
	protected void parse0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		m_topic = codec.readString();
		m_partition = codec.readInt();
		m_time = codec.readLong();
	}

	@Override
	protected void toBytes0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		codec.writeString(m_topic);
		codec.writeInt(m_partition);
		codec.writeLong(m_time);
	}

	@Override
	public String toString() {
		return "QueryMessageOffsetCommand [m_topic=" + m_topic + ", m_partition=" + m_partition + "]";
	}
}
