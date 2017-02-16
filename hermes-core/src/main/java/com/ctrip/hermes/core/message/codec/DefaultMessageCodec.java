package com.ctrip.hermes.core.message.codec;

import io.netty.buffer.ByteBuf;

import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.message.BaseConsumerMessage;
import com.ctrip.hermes.core.message.PartialDecodedMessage;
import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.transport.netty.Magic;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = MessageCodec.class)
public class DefaultMessageCodec implements MessageCodec {
	private static MessageCodecVersion CURRENT_VERSION = MessageCodecVersion.BINARY_V1;
	
	@Override
	public void encode(ProducerMessage<?> msg, ByteBuf buf) {
		Magic.writeMagic(buf);
		buf.writeByte(CURRENT_VERSION.getVersion());

		CURRENT_VERSION.getHandler().encode(msg, buf);
	}

	@Override
	public byte[] encode(ProducerMessage<?> msg) {
		return CURRENT_VERSION.getHandler().encode(msg, CURRENT_VERSION.getVersion());
	}

	@Override
	public PartialDecodedMessage decodePartial(ByteBuf buf) {
		Magic.readAndCheckMagic(buf);
		MessageCodecVersion version = getVersion(buf);
		return version.getHandler().decodePartial(buf);
	}

	@Override
	public BaseConsumerMessage<?> decode(String topic, ByteBuf buf, Class<?> bodyClazz) {
		Magic.readAndCheckMagic(buf);
		MessageCodecVersion version = getVersion(buf);
		return version.getHandler().decode(topic, buf, bodyClazz);
	}

	@Override
	public void encodePartial(PartialDecodedMessage msg, ByteBuf buf) {
		Magic.writeMagic(buf);
		buf.writeByte(CURRENT_VERSION.getVersion());

		CURRENT_VERSION.getHandler().encode(msg, buf);
	}

	private MessageCodecVersion getVersion(ByteBuf buf) {
		byte versionByte = buf.readByte();
		MessageCodecVersion version = MessageCodecVersion.valueOf(versionByte);
		if (version == null) {
			throw new IllegalArgumentException(String.format("Unknown version %d", versionByte));
		}

		return version;
	}

}
