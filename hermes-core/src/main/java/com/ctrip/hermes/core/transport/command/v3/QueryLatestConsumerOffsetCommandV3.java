package com.ctrip.hermes.core.transport.command.v3;

import io.netty.buffer.ByteBuf;

import com.ctrip.hermes.core.transport.command.AbstractCommand;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;
import com.google.common.util.concurrent.SettableFuture;

public class QueryLatestConsumerOffsetCommandV3 extends AbstractCommand {

	private static final long serialVersionUID = -5968280482970892243L;

	private String m_topic;

	private int m_partition;

	private String m_groupId;

	private transient SettableFuture<QueryOffsetResultCommandV3> m_future;

	public QueryLatestConsumerOffsetCommandV3() {
		this(null, -1, null);
	}

	public QueryLatestConsumerOffsetCommandV3(String topic, int partition, String groupId) {
		super(CommandType.QUERY_LATEST_CONSUMER_OFFSET_V3, 3);
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

	public SettableFuture<QueryOffsetResultCommandV3> getFuture() {
		return m_future;
	}

	public void setFuture(SettableFuture<QueryOffsetResultCommandV3> future) {
		m_future = future;
	}

	public void onResultReceived(QueryOffsetResultCommandV3 result) {
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

}
