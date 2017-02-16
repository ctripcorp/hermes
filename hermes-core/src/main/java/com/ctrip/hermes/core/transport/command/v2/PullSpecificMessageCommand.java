package com.ctrip.hermes.core.transport.command.v2;

import io.netty.buffer.ByteBuf;

import java.util.List;

import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;

public class PullSpecificMessageCommand extends AbstractPullMessageCommand {

	private static final long serialVersionUID = 4075046802809509104L;

	private List<Offset> m_offsets;

	public PullSpecificMessageCommand() {
		this(null, -1, null, -1);
	}

	public PullSpecificMessageCommand(String topic, int partition, List<Offset> offsets, long expireTime) {
		super(CommandType.MESSAGE_PULL_SPECIFIC, 1, topic, partition, expireTime);
		m_offsets = offsets;
	}

	public long getExpireTime() {
		return m_expireTime;
	}

	public List<Offset> getOffsets() {
		return m_offsets;
	}

	public String getTopic() {
		return m_topic;
	}

	public int getPartition() {
		return m_partition;
	}

	@Override
	protected void toBytes0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);

		codec.writeString(m_topic);
		codec.writeInt(m_partition);
		codec.writeLong(m_expireTime);
		codec.writeOffsets(m_offsets);
	}

	@Override
	protected void parse0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);

		m_topic = codec.readString();
		m_partition = codec.readInt();
		m_expireTime = codec.readLong();
		m_offsets = codec.readOffsets();
	}

	@Override
	public String toString() {
		return "PullSpecificMessageCommand [m_offsets=" + m_offsets + ", m_topic=" + m_topic + ", m_partition="
		      + m_partition + ", m_expireTime=" + m_expireTime + "]";
	}
}
