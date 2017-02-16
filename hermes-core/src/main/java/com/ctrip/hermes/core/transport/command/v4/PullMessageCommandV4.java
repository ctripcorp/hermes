package com.ctrip.hermes.core.transport.command.v4;

import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.v2.AbstractPullMessageCommand;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;

import io.netty.buffer.ByteBuf;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class PullMessageCommandV4 extends AbstractPullMessageCommand {

	private static final long serialVersionUID = 7802590791501011408L;

	private String m_groupId;

	private Offset m_offset;

	private int m_size;

	private String m_filter;

	public PullMessageCommandV4() {
		this(null, -1, null, null, 0, -1L, null);
	}

	public PullMessageCommandV4(String topic, int partition, String groupId, Offset offset, int size, long expireTime,
	      String filter) {
		super(CommandType.MESSAGE_PULL_V4, 4, topic, partition, expireTime);
		m_groupId = groupId;
		m_offset = offset;
		m_size = size;
		m_filter = filter;
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

	public String getFilter() {
		return m_filter;
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
		m_filter = codec.readString();
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
		codec.writeString(m_filter);
	}

	@Override
	public String toString() {
		return "PullMessageCommandV4 [m_groupId=" + m_groupId + ", m_offset=" + m_offset + ", m_size=" + m_size
		      + ", m_topic=" + m_topic + ", m_partition=" + m_partition + ", m_expireTime=" + m_expireTime
		      + ", m_filter=" + m_filter + ", m_header=" + m_header + "]";
	}

}
