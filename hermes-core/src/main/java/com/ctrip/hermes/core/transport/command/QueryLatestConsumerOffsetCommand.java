package com.ctrip.hermes.core.transport.command;

import io.netty.buffer.ByteBuf;

import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;
import com.google.common.util.concurrent.SettableFuture;

public class QueryLatestConsumerOffsetCommand extends AbstractCommand {

	private static final long serialVersionUID = -5558419534493044888L;

	private String m_topic;

	private int m_partition;

	private String m_groupId;

	private transient SettableFuture<QueryOffsetResultCommand> m_future;

	public QueryLatestConsumerOffsetCommand() {
		this(null, -1, null);
	}

	public QueryLatestConsumerOffsetCommand(String topic, int partition, String groupId) {
		super(CommandType.QUERY_LATEST_CONSUMER_OFFSET, 1);
		m_topic = topic;
		m_partition = partition;
		m_groupId = groupId;
	}

	public String getTopic() {
		return m_topic;
	}

	public int getPartition() {
		return m_partition;
	}

	public String getGroupId() {
		return m_groupId;
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
		m_groupId = codec.readString();
	}

	@Override
	protected void toBytes0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		codec.writeString(m_topic);
		codec.writeInt(m_partition);
		codec.writeString(m_groupId);
	}

	@Override
	public String toString() {
		return "QueryConsumerOffsetCommand [m_topic=" + m_topic + ", m_partition=" + m_partition + ", m_groupId="
		      + m_groupId + "]";
	}
}
