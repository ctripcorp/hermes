package com.ctrip.hermes.core.transport.command;

import io.netty.buffer.ByteBuf;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.ctrip.hermes.core.transport.netty.Magic;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class Header implements Serializable {
	private static final long serialVersionUID = -8403214524035724226L;

	private static AtomicLong CorrelationId = new AtomicLong(0);

	private int m_version = 1;

	private CommandType m_type;

	private long m_correlationId = CorrelationId.getAndIncrement();

	private Map<String, String> m_properties = new HashMap<String, String>();

	public int getVersion() {
		return m_version;
	}

	public void setVersion(int version) {
		m_version = version;
	}

	public CommandType getType() {
		return m_type;
	}

	public void setType(CommandType type) {
		m_type = type;
	}

	public long getCorrelationId() {
		return m_correlationId;
	}

	public void setCorrelationId(long correlationId) {
		m_correlationId = correlationId;
	}

	public Map<String, String> getProperties() {
		return m_properties;
	}

	public void setProperties(Map<String, String> properties) {
		m_properties = properties;
	}

	public void addProperty(String key, String value) {
		m_properties.put(key, value);
	}

	public void parse(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		Magic.readAndCheckMagic(buf);
		m_version = codec.readInt();
		m_type = CommandType.valueOf(codec.readInt(), m_version);
		if (m_type == null) {
			throw new IllegalArgumentException("Command type can not be null.");
		}
		m_correlationId = codec.readLong();
		m_properties = codec.readStringStringMap();
	}

	public void toBytes(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		Magic.writeMagic(buf);
		codec.writeInt(m_version);
		codec.writeInt(m_type.getType());
		codec.writeLong(m_correlationId);
		codec.writeStringStringMap(m_properties);
	}

	@Override
	public String toString() {
		return "Header [m_version=" + m_version + ", m_type=" + m_type + ", m_correlationId=" + m_correlationId
		      + ", m_properties=" + m_properties + "]";
	}

}
