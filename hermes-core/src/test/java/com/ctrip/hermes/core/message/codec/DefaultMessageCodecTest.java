package com.ctrip.hermes.core.message.codec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.HermesCoreBaseTest;
import com.ctrip.hermes.core.message.BaseConsumerMessage;
import com.ctrip.hermes.core.message.PartialDecodedMessage;
import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.message.PropertiesHolder;
import com.ctrip.hermes.core.message.payload.GZipPayloadCodec;
import com.ctrip.hermes.core.message.payload.JsonPayloadCodec;
import com.ctrip.hermes.core.message.payload.PayloadCodecCompositor;
import com.ctrip.hermes.core.message.payload.RawMessage;
import com.ctrip.hermes.core.transport.netty.Magic;
import com.ctrip.hermes.meta.entity.Codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class DefaultMessageCodecTest extends HermesCoreBaseTest {

	@Test
	public void test() {
		byte[] bytes = "123".getBytes();

		PayloadCodecCompositor codec = new PayloadCodecCompositor("json,gzip");
		codec.addPayloadCodec(new JsonPayloadCodec());
		codec.addPayloadCodec(new GZipPayloadCodec());

		byte[] encodedBytes = codec.encode("", new RawMessage(bytes));

		Assert.assertEquals("123", codec.decode(encodedBytes, String.class));
	}

	@Test
	public void testEncodeWithProducerMsgAndPartialDecodeWithStringBody() throws Exception {
		long bornTime = System.currentTimeMillis();
		ProducerMessage<String> msg = createProducerMessage("topic", "body", "key", 10, 10, "pKey", bornTime, true,
				true, Arrays.asList(new Pair<String, String>("a", "A")),
				Arrays.asList(new Pair<String, String>("b", "B")), Arrays.asList(new Pair<String, String>("c", "C")));

		DefaultMessageCodec codec = new DefaultMessageCodec();

		byte[] bytes = codec.encode(msg);

		PartialDecodedMessage pmsg = codec.decodePartial(Unpooled.wrappedBuffer(bytes));

		assertEquals(msg.getBornTime(), pmsg.getBornTime());
		assertEquals(Codec.JSON, pmsg.getBodyCodecType());
		assertEquals(msg.getKey(), pmsg.getKey());
		assertEquals(msg.getBody(), new JsonPayloadCodec().decode(pmsg.readBody(), String.class));
		assertProeprties(readProperties(pmsg.getDurableProperties()),
				Arrays.asList(new Pair<String, String>(PropertiesHolder.APP + "a", "A"),
						new Pair<String, String>(PropertiesHolder.SYS + "b", "B")));
		assertProeprties(readProperties(pmsg.getVolatileProperties()),
				Arrays.asList(new Pair<String, String>("c", "C")));
	}

	@Test
	public void testEncodeWithProducerMsgAndPartialDecodeWithStringBodyAndNullProperties() throws Exception {
		long bornTime = System.currentTimeMillis();
		ProducerMessage<String> msg = createProducerMessage("topic", "body", "key", 10, 10, "pKey", bornTime, true,
				true, null, null, null);

		DefaultMessageCodec codec = new DefaultMessageCodec();

		byte[] bytes = codec.encode(msg);

		PartialDecodedMessage pmsg = codec.decodePartial(Unpooled.wrappedBuffer(bytes));

		assertEquals(msg.getBornTime(), pmsg.getBornTime());
		assertEquals(Codec.JSON, pmsg.getBodyCodecType());
		assertEquals(msg.getKey(), pmsg.getKey());
		assertEquals(msg.getBody(), new JsonPayloadCodec().decode(pmsg.readBody(), String.class));

		assertProeprties(readProperties(pmsg.getDurableProperties()), Collections.<Pair<String, String>>emptyList());
		assertProeprties(readProperties(pmsg.getVolatileProperties()), Collections.<Pair<String, String>>emptyList());
	}

	@Test
	public void testEncodeWithBufAndPartialDecodeWithStringBody() throws Exception {
		long bornTime = System.currentTimeMillis();
		ProducerMessage<String> msg = createProducerMessage("topic", "body", "key", 10, 10, "pKey", bornTime, true,
				true, Arrays.asList(new Pair<String, String>("a", "A")),
				Arrays.asList(new Pair<String, String>("b", "B")), Arrays.asList(new Pair<String, String>("c", "C")));

		DefaultMessageCodec codec = new DefaultMessageCodec();

		ByteBuf buffer = Unpooled.buffer();

		codec.encode(msg, buffer);

		PartialDecodedMessage pmsg = codec.decodePartial(buffer);

		assertEquals(msg.getBornTime(), pmsg.getBornTime());
		assertEquals(Codec.JSON, pmsg.getBodyCodecType());
		assertEquals(msg.getKey(), pmsg.getKey());
		assertEquals(msg.getBody(), new JsonPayloadCodec().decode(pmsg.readBody(), String.class));
		assertProeprties(readProperties(pmsg.getDurableProperties()),
				Arrays.asList(new Pair<String, String>(PropertiesHolder.APP + "a", "A"),
						new Pair<String, String>(PropertiesHolder.SYS + "b", "B")));
		assertProeprties(readProperties(pmsg.getVolatileProperties()),
				Arrays.asList(new Pair<String, String>("c", "C")));
	}

	@Test
	public void testEncodePartialAndDecodeWithStringBody() throws Exception {
		long bornTime = System.currentTimeMillis();

		PartialDecodedMessage pmsg = new PartialDecodedMessage();
		pmsg.setBodyCodecType(Codec.JSON);
		pmsg.setBornTime(bornTime);
		pmsg.setKey("key");
		pmsg.setBody(Unpooled.wrappedBuffer(new JsonPayloadCodec().encode("topic", "hello")));
		ByteBuf durablePropsBuf = Unpooled.buffer();
		writeProperties(Arrays.asList(new Pair<String, String>("a", "A"), new Pair<String, String>("b", "B")),
				durablePropsBuf);
		pmsg.setDurableProperties(durablePropsBuf);
		ByteBuf volatilePropsBuf = Unpooled.buffer();
		writeProperties(Arrays.asList(new Pair<String, String>("c", "C")), volatilePropsBuf);
		pmsg.setVolatileProperties(volatilePropsBuf);

		DefaultMessageCodec codec = new DefaultMessageCodec();
		ByteBuf buf = Unpooled.buffer();
		codec.encodePartial(pmsg, buf);

		BaseConsumerMessage<?> cmsg = codec.decode("topic", buf, String.class);

		assertEquals(bornTime, cmsg.getBornTime());
		assertEquals("hello", cmsg.getBody());
		assertEquals("key", cmsg.getRefKey());
		assertEquals("topic", cmsg.getTopic());

		assertProeprties(cmsg.getPropertiesHolder().getDurableProperties(),
				Arrays.asList(new Pair<String, String>("a", "A"), new Pair<String, String>("b", "B")));
		assertProeprties(cmsg.getPropertiesHolder().getVolatileProperties(),
				Arrays.asList(new Pair<String, String>("c", "C")));
	}

	@Test
	public void testEncodePartialAndDecodeWithStringBodyAndNullProperties() throws Exception {
		long bornTime = System.currentTimeMillis();

		PartialDecodedMessage pmsg = new PartialDecodedMessage();
		pmsg.setBodyCodecType(Codec.JSON);
		pmsg.setBornTime(bornTime);
		pmsg.setKey("key");
		pmsg.setBody(Unpooled.wrappedBuffer(new JsonPayloadCodec().encode("topic", "hello")));
		pmsg.setDurableProperties(null);
		pmsg.setVolatileProperties(null);

		DefaultMessageCodec codec = new DefaultMessageCodec();
		ByteBuf buf = Unpooled.buffer();
		codec.encodePartial(pmsg, buf);

		BaseConsumerMessage<?> cmsg = codec.decode("topic", buf, String.class);

		assertEquals(bornTime, cmsg.getBornTime());
		assertEquals("hello", cmsg.getBody());
		assertEquals("key", cmsg.getRefKey());
		assertEquals("topic", cmsg.getTopic());

		assertProeprties(cmsg.getPropertiesHolder().getDurableProperties(),
				Collections.<Pair<String, String>>emptyList());
		assertProeprties(cmsg.getPropertiesHolder().getVolatileProperties(),
				Collections.<Pair<String, String>>emptyList());
	}

	@Test
	public void testEncodeAndDecodeWithStringBody() throws Exception {
		long bornTime = System.currentTimeMillis();
		ProducerMessage<String> msg = createProducerMessage("topic", "body", "key", 10, 10, "pKey", bornTime, true,
				true, Arrays.asList(new Pair<String, String>("a", "A")),
				Arrays.asList(new Pair<String, String>("b", "B")), Arrays.asList(new Pair<String, String>("c", "C")));

		DefaultMessageCodec codec = new DefaultMessageCodec();

		byte[] bytes = codec.encode(msg);

		BaseConsumerMessage<?> cmsg = codec.decode("topic", Unpooled.wrappedBuffer(bytes), String.class);

		assertEquals(bornTime, cmsg.getBornTime());
		assertEquals("body", cmsg.getBody());
		assertEquals("key", cmsg.getRefKey());
		assertEquals("topic", cmsg.getTopic());

		assertProeprties(cmsg.getPropertiesHolder().getDurableProperties(),
				Arrays.asList(new Pair<String, String>(PropertiesHolder.APP + "a", "A"),
						new Pair<String, String>(PropertiesHolder.SYS + "b", "B")));
		assertProeprties(cmsg.getPropertiesHolder().getVolatileProperties(),
				Arrays.asList(new Pair<String, String>("c", "C")));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testMagicNumberCheckFail() throws Exception {
		ByteBuf buf = Unpooled.buffer();
		buf.writeBytes(new byte[] { 1, 2, 3, 4 });
		MessageCodec codec = new DefaultMessageCodec();
		codec.decode("topic", buf, String.class);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testUnknowVersion() throws Exception {
		ByteBuf buf = Unpooled.buffer();
		Magic.writeMagic(buf);
		buf.writeByte(100);
		MessageCodec codec = new DefaultMessageCodec();
		codec.decode("topic", buf, String.class);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testCRCFail() throws Exception {
		ByteBuf buf = Unpooled.buffer();
		Magic.writeMagic(buf);
		buf.writeByte(MessageCodecVersion.BINARY_V1.getVersion());
		buf.writeInt(30);
		buf.writeInt(1);
		buf.writeInt(1);
		buf.writeBytes(new byte[] { 1, 2, 1 });
		buf.writeLong(10L);
		MessageCodec codec = new DefaultMessageCodec();
		codec.decode("topic", buf, String.class);
	}

	private ProducerMessage<String> createProducerMessage(String topic, String body, String key, int seq, int partition,
			String partitionKey, long bornTime, boolean isPriority, boolean withHeader,
			List<Pair<String, String>> appProperites, List<Pair<String, String>> sysProperites,
			List<Pair<String, String>> volatileProperites) {
		ProducerMessage<String> msg = new ProducerMessage<String>(topic, body);
		msg.setBornTime(bornTime);
		msg.setKey(key);
		msg.setMsgSeqNo(seq);
		msg.setPartition(partition);
		msg.setPartitionKey(partitionKey);
		msg.setPriority(isPriority);
		msg.setWithCatTrace(withHeader);
		PropertiesHolder propertiesHolder = new PropertiesHolder();
		if (appProperites != null && !appProperites.isEmpty()) {
			for (Pair<String, String> appProperty : appProperites) {
				propertiesHolder.addDurableAppProperty(appProperty.getKey(), appProperty.getValue());
			}
		}
		if (sysProperites != null && !sysProperites.isEmpty()) {
			for (Pair<String, String> sysProperty : sysProperites) {
				propertiesHolder.addDurableSysProperty(sysProperty.getKey(), sysProperty.getValue());
			}
		}
		if (volatileProperites != null && !volatileProperites.isEmpty()) {
			for (Pair<String, String> volatileProperty : volatileProperites) {
				propertiesHolder.addVolatileProperty(volatileProperty.getKey(), volatileProperty.getValue());
			}
		}
		msg.setPropertiesHolder(propertiesHolder);
		return msg;
	}

	private void assertProeprties(Map<String, String> actual, List<Pair<String, String>> expects) {
		if (expects == null) {
			assertNull(actual);
		} else {
			assertEquals(actual.size(), expects.size());
			for (Pair<String, String> expPair : expects) {
				assertEquals(expPair.getValue(), actual.get(expPair.getKey()));
			}
		}
	}

}
