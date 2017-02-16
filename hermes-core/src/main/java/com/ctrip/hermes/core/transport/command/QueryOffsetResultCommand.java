package com.ctrip.hermes.core.transport.command;

import io.netty.buffer.ByteBuf;

import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;

public class QueryOffsetResultCommand extends AbstractCommand {

	private static final long serialVersionUID = 1248074244176061444L;

	private Offset m_offset;

	public Offset getOffset() {
		return m_offset;
	}

	public QueryOffsetResultCommand() {
		this(null);
	}

	public QueryOffsetResultCommand(Offset offset) {
		super(CommandType.RESULT_QUERY_OFFSET, 1);
		m_offset = offset;
	}

	@Override
	protected void parse0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		m_offset = codec.readOffset();
	}

	@Override
	protected void toBytes0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		codec.writeOffset(m_offset);
	}

	@Override
	public String toString() {
		return "QueryOffsetResultCommand [m_offset=" + m_offset + "]";
	}
}
