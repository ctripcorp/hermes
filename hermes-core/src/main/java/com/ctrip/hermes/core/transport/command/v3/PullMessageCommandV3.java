package com.ctrip.hermes.core.transport.command.v3;

import io.netty.buffer.ByteBuf;

import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.v2.AbstractPullMessageCommand;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class PullMessageCommandV3 extends AbstractPullMessageCommand {

	private static final long serialVersionUID = 7690048178447687657L;

	private String m_groupId;

	private Offset m_offset;

	private int m_size;

	public PullMessageCommandV3() {
		this(null, -1, null, null, 0, -1L);
	}

	public PullMessageCommandV3(String topic, int partition, String groupId, Offset offset, int size, long expireTime) {
		super(CommandType.MESSAGE_PULL_V3, 3, topic, partition, expireTime);
		m_groupId = groupId;
		m_offset = offset;
		m_size = size;
	}

	public Offset getOffset() {
		return m_offset;
	}

	public int getSize() {
		return m_size;
	}

	public String getGroupId() {
		return m_groupId;
	}

	@Override
	public void parse0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);

		m_topic = codec.readString();
		m_partition = codec.readInt();
		m_groupId = codec.readString();
		m_offset = codec.readOffset();
		m_size = codec.readInt();
		m_expireTime = codec.readLong();
	}

	@Override
	public void toBytes0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);

		codec.writeString(m_topic);
		codec.writeInt(m_partition);
		codec.writeString(m_groupId);
		codec.writeOffset(m_offset);
		codec.writeInt(m_size);
		codec.writeLong(m_expireTime);
	}

	@Override
	public String toString() {
		return "PullMessageCommandV3 [m_groupId=" + m_groupId + ", m_offset=" + m_offset + ", m_size=" + m_size
		      + ", m_topic=" + m_topic + ", m_partition=" + m_partition + ", m_expireTime=" + m_expireTime
		      + ", m_header=" + m_header + "]";
	}

}
