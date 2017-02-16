package com.ctrip.hermes.core.transport.command.v6;

import io.netty.buffer.ByteBuf;

import com.ctrip.hermes.core.transport.command.AbstractCommand;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class FetchManualConfigCommandV6 extends AbstractCommand {

	private static final long serialVersionUID = 5507922065276703281L;

	public static final long UNSET_VERSION = -1L;

	private long m_version = UNSET_VERSION;

	public FetchManualConfigCommandV6() {
		super(CommandType.FETCH_MANUAL_CONFIG_V6, 6);
	}

	public long getVersion() {
		return m_version;
	}

	public void setVersion(long version) {
		m_version = version;
	}

	@Override
	public void parse0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		m_version = codec.readLong();
	}

	@Override
	public void toBytes0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		codec.writeLong(m_version);
	}

}
