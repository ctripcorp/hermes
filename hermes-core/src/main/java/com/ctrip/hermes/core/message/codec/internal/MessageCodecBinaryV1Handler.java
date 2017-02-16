package com.ctrip.hermes.core.message.codec.internal;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.Map;

import com.ctrip.hermes.core.message.BaseConsumerMessage;
import com.ctrip.hermes.core.message.DummyBaseConsumerMessage;
import com.ctrip.hermes.core.message.PartialDecodedMessage;
import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.message.PropertiesHolder;
import com.ctrip.hermes.core.message.codec.MessageCodecHandler;
import com.ctrip.hermes.core.message.payload.PayloadCodec;
import com.ctrip.hermes.core.message.payload.PayloadCodecFactory;
import com.ctrip.hermes.core.transport.DummyMessage;
import com.ctrip.hermes.core.transport.netty.Magic;
import com.ctrip.hermes.core.utils.ChecksumUtil;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class MessageCodecBinaryV1Handler implements MessageCodecHandler {

	@Override
	public byte[] encode(ProducerMessage<?> msg, byte version) {
		PayloadCodec bodyCodec = PayloadCodecFactory.getCodecByTopicName(msg.getTopic());
		byte[] body = bodyCodec.encode(msg.getTopic(), msg.getBody());
		// TODO better estimate buf size
		ByteBuf buf = Unpooled.buffer(body.length + 150);
		Magic.writeMagic(buf);
		buf.writeByte(version);

		encode(msg, buf, body, bodyCodec.getType());

		byte[] res = new byte[buf.readableBytes()];

		buf.readBytes(res);

		return res;
	}

	@Override
	public void encode(ProducerMessage<?> msg, ByteBuf buf) {
		PayloadCodec bodyCodec = PayloadCodecFactory.getCodecByTopicName(msg.getTopic());
		byte[] body = bodyCodec.encode(msg.getTopic(), msg.getBody());
		encode(msg, buf, body, bodyCodec.getType());
	}

	private void encode(ProducerMessage<?> msg, ByteBuf buf, byte[] body, String codecType) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);

		int indexBeginning = buf.writerIndex();
		codec.writeInt(-1);// placeholder for whole length
		int indexAfterWholeLen = buf.writerIndex();
		codec.writeInt(-1);// placeholder for header length
		codec.writeInt(-1);// placeholder for body length
		int indexBeforeHeader = buf.writerIndex();

		// header begin
		codec.writeString(msg.getKey());
		codec.writeLong(msg.getBornTime());
		codec.writeInt(0);// remaining retries
		codec.writeString(codecType);
		PropertiesHolder propertiesHolder = msg.getPropertiesHolder();
		writeProperties(propertiesHolder.getDurableProperties(), buf, codec);
		writeProperties(propertiesHolder.getVolatileProperties(), buf, codec);
		// header end

		int headerLen = buf.writerIndex() - indexBeforeHeader;

		// body begin
		int indexBeforeBody = buf.writerIndex();
		buf.writeBytes(body);
		int bodyLen = buf.writerIndex() - indexBeforeBody;
		// body end

		// crc
		codec.writeLong(ChecksumUtil.crc32(buf.slice(indexBeforeHeader, headerLen + bodyLen)));
		int indexEnd = buf.writerIndex();

		int wholeLen = indexEnd - indexAfterWholeLen;

		// refill whole length
		buf.writerIndex(indexBeginning);
		codec.writeInt(wholeLen);

		// refill header length
		codec.writeInt(headerLen);

		// refill body length
		codec.writeInt(bodyLen);

		buf.writerIndex(indexEnd);

	}

	@Override
	public PartialDecodedMessage decodePartial(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);

		// skip whole length
		codec.readInt();
		// skip header length
		int headerLen = codec.readInt();
		// skip body length
		int bodyLen = codec.readInt();
		verifyChecksum(buf, headerLen + bodyLen);
		PartialDecodedMessage msg = new PartialDecodedMessage();
		msg.setKey(codec.readString());
		msg.setBornTime(codec.readLong());
		msg.setRemainingRetries(codec.readInt());
		msg.setBodyCodecType(codec.readString());

		int len = codec.readInt();
		msg.setDurableProperties(buf.readSlice(len));

		len = codec.readInt();
		msg.setVolatileProperties(buf.readSlice(len));

		msg.setBody(buf.readSlice(bodyLen));

		// skip crc
		codec.readLong();

		return msg;
	}

	private void verifyChecksum(ByteBuf buf, int len) {
		// long actualChecksum = ChecksumUtil.crc32(buf.slice(buf.readerIndex(), len));
		// long expectedChecksum = buf.getLong(buf.readerIndex() + len);
		// if (actualChecksum != expectedChecksum) {
		// throw new IllegalArgumentException("checksum mismatch");
		// }
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public BaseConsumerMessage<?> decode(String topic, ByteBuf buf, Class<?> bodyClazz) {
		PartialDecodedMessage decodedMessage = decodePartial(buf);
		Map<String, String> durableProperties = readProperties(decodedMessage.getDurableProperties());
		Map<String, String> volatileProperties = readProperties(decodedMessage.getVolatileProperties());
		PropertiesHolder propertiesHolder = new PropertiesHolder(durableProperties, volatileProperties);

		BaseConsumerMessage msg = DummyMessage.DUMMY_HEADER_VALUE.equals(propertiesHolder.getDurableProperties().get(
		      DummyMessage.DUMMY_HEADER_KEY)) ? new DummyBaseConsumerMessage<>() : new BaseConsumerMessage<>();
		msg.setTopic(topic);
		msg.setRefKey(decodedMessage.getKey());
		msg.setBornTime(decodedMessage.getBornTime());
		msg.setRemainingRetries(decodedMessage.getRemainingRetries());
		msg.setPropertiesHolder(propertiesHolder);
		if (!(msg instanceof DummyBaseConsumerMessage)) {
			PayloadCodec bodyCodec = PayloadCodecFactory.getCodecByType(decodedMessage.getBodyCodecType());
			msg.setBody(bodyCodec.decode(decodedMessage.readBody(), bodyClazz));
		}
		return msg;
	}

	@Override
	public void encode(PartialDecodedMessage msg, ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);

		int indexBeginning = buf.writerIndex();
		codec.writeInt(-1);// placeholder for whole length
		int indexAfterWholeLen = buf.writerIndex();
		codec.writeInt(-1);// placeholder for header length
		codec.writeInt(-1);// placeholder for body length
		int indexBeforeHeader = buf.writerIndex();

		// header begin
		codec.writeString(msg.getKey());
		codec.writeLong(msg.getBornTime());
		codec.writeInt(msg.getRemainingRetries());
		codec.writeString(msg.getBodyCodecType());
		writeProperties(msg.getDurableProperties(), buf, codec);
		writeProperties(msg.getVolatileProperties(), buf, codec);
		// header end

		int headerLen = buf.writerIndex() - indexBeforeHeader;

		// body begin
		ByteBuf body = msg.getBody();
		int bodyLen = body.readableBytes();
		buf.writeBytes(body);
		// body end

		// crc
		codec.writeLong(ChecksumUtil.crc32(buf.slice(indexBeforeHeader, headerLen + bodyLen)));
		int indexEnd = buf.writerIndex();

		// refill whole length
		buf.writerIndex(indexBeginning);
		codec.writeInt(indexEnd - indexAfterWholeLen);
		// refill header length
		codec.writeInt(headerLen);
		// refill body length
		codec.writeInt(bodyLen);

		buf.writerIndex(indexEnd);
	}

	private void writeProperties(ByteBuf propertiesBuf, ByteBuf out, HermesPrimitiveCodec codec) {
		int writeIndexBeforeLength = out.writerIndex();
		codec.writeInt(-1);
		int writeIndexBeforeMap = out.writerIndex();
		if (propertiesBuf != null) {
			out.writeBytes(propertiesBuf);
		} else {
			codec.writeNull();
		}
		int mapLength = out.writerIndex() - writeIndexBeforeMap;
		int writeIndexEnd = out.writerIndex();
		out.writerIndex(writeIndexBeforeLength);
		codec.writeInt(mapLength);
		out.writerIndex(writeIndexEnd);
	}

	private void writeProperties(Map<String, String> properties, ByteBuf buf, HermesPrimitiveCodec codec) {
		int writeIndexBeforeLength = buf.writerIndex();
		codec.writeInt(-1);
		int writeIndexBeforeMap = buf.writerIndex();
		codec.writeStringStringMap(properties);
		int mapLength = buf.writerIndex() - writeIndexBeforeMap;
		int writeIndexEnd = buf.writerIndex();
		buf.writerIndex(writeIndexBeforeLength);
		codec.writeInt(mapLength);
		buf.writerIndex(writeIndexEnd);
	}

	private Map<String, String> readProperties(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		return codec.readStringStringMap();
	}

}
