package com.ctrip.hermes.core.transport.command.v2;

import io.netty.buffer.ByteBuf;

import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class PullMessageCommandV2 extends AbstractPullMessageCommand {
	private static final long serialVersionUID = 6338481458963711634L;

	public static final int PULL_WITH_OFFSET = 1;

	public static final int PULL_WITHOUT_OFFSET = 2;

	private int m_pullType = PULL_WITH_OFFSET;

	private String m_groupId;

	private Offset m_offset;

	private int m_size;

	public PullMessageCommandV2() {
		this(-1, null, -1, null, null, 0, -1L);
	}

	public PullMessageCommandV2(int pullType, String topic, int partition, String groupId, Offset offset, int size,
	      long expireTime) {
		super(CommandType.MESSAGE_PULL_V2, 2, topic, partition, expireTime);
		m_pullType = pullType;
		m_groupId = groupId;
		m_offset = offset;
		m_size = size;
	}

	public int getPullType() {
		return m_pullType;
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

		m_pullType = codec.readInt();
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

		codec.writeInt(m_pullType);
		codec.writeString(m_topic);
		codec.writeInt(m_partition);
		codec.writeString(m_groupId);
		codec.writeOffset(m_offset);
		codec.writeInt(m_size);
		codec.writeLong(m_expireTime);
	}

	@Override
	public String toString() {
		return "PullMessageCommandV2 [m_pullType=" + m_pullType + ", m_groupId=" + m_groupId + ", m_topic=" + m_topic
		      + ", m_partition=" + m_partition + ", m_offset=" + m_offset + ", m_size=" + m_size + ", m_expireTime="
		      + m_expireTime + "]";
	}
}
