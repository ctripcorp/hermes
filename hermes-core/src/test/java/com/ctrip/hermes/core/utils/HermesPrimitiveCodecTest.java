package com.ctrip.hermes.core.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;

public class HermesPrimitiveCodecTest extends ComponentTestCase {

	final static int dataSize = 10000;

	@Test
	public void testInt() {
		ByteBuf bf = Unpooled.buffer();

		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);
		codec.writeInt(5);
		codec.writeInt(1088);
		assertEquals(5, codec.readInt());
		assertEquals(1088, codec.readInt());
	}

	@Test
	public void testBytes() {
		ByteBuf bf = Unpooled.buffer();

		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);

		byte[] bytes = new byte[] { 'a', 'c', 'f', 'u', 'n' };
		codec.writeBytes(bytes);
		byte[] result = codec.readBytes();
		for (int i = 0; i < result.length; i++) {
			assertEquals(bytes[i], result[i]);
		}
	}

	@Test
	public void testChar() {
		ByteBuf bf = Unpooled.buffer();

		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);
		codec.writeChar('a');
		codec.writeChar('c');
		codec.writeChar('f');
		codec.writeChar('u');
		codec.writeChar('n');
		assertEquals('a', codec.readChar());
		assertEquals('c', codec.readChar());
		assertEquals('f', codec.readChar());
		assertEquals('u', codec.readChar());
		assertEquals('n', codec.readChar());
	}

	@Test
	public void testLong() {
		ByteBuf bf = Unpooled.buffer();

		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);
		codec.writeLong(5);
		codec.writeLong(1088);
		codec.writeLong(23918237891724L);
		assertEquals(5, codec.readLong());
		assertEquals(1088, codec.readLong());
		assertEquals(23918237891724L, codec.readLong());
	}

	@Test
	public void testBoolean() {
		ByteBuf bf = Unpooled.buffer();

		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);
		codec.writeBoolean(true);
		codec.writeBoolean(false);
		codec.writeBoolean(false);
		assertEquals(true, codec.readBoolean());
		assertEquals(false, codec.readBoolean());
		assertEquals(false, codec.readBoolean());
	}

	@Test
	public void testNullBytes() {
		ByteBuf bf = Unpooled.buffer();

		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);
		byte[] bytes = null;
		codec.writeBytes(bytes);
		assertNull(codec.readBytes());
	}

	@Test
	public void testNullStringStringMap() {
		ByteBuf bf = Unpooled.buffer();

		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);
		codec.writeStringStringMap(null);
		assertNull(codec.readStringStringMap());
	}

	@Test
	public void testNullLongIntMap() {
		ByteBuf bf = Unpooled.buffer();

		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);
		codec.writeLongIntMap(null);
		assertNull(codec.readLongIntMap());
	}

	@Test
	public void testNullIntBooleanMap() {
		ByteBuf bf = Unpooled.buffer();

		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);
		codec.writeIntBooleanMap(null);
		assertNull(codec.readIntBooleanMap());
	}

	@Test
	public void testNullString() {
		ByteBuf bf = Unpooled.buffer();

		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);
		codec.writeString(null);
		assertNull(codec.readString());
	}

	@Test
	public void testString() {
		String a = null;
		String b = "longer string";
		String c = "中文字very very long string \n \r \n \b." + "very very long string \n" + " \n" + " \n" + " \b.";
		String strangeString = "{\"1\":{\"str\":\"429bb071\"},"
		      + "\"2\":{\"s\":\"ExchangeTest\"},\"3\":{\"i32\":8},\"4\":{\"str\":\"uft-8\"},"
		      + "\"5\":{\"str\":\"cmessage-adapter 1.0\"},\"6\":{\"i32\":3},\"7\":{\"i32\":1},"
		      + "\"8\":{\"i32\":0},\"9\":{\"str\":\"order_new\"},\"10\":{\"str\":\"\"},"
		      + "\"11\":{\"str\":\"1\"},\"12\":{\"str\":\"DST56615\"},\"13\":{\"str\":\"555555\"},"
		      + "\"14\":{\"str\":\"169.254.142.159\"},\"15\":{\"str\":\"java.lang.String\"},"
		      + "\"16\":{\"i64\":1429168996889},\"17\":{\"map\":[\"str\",\"str\",0,{}]}}";

		ByteBuf bf = Unpooled.buffer();

		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(bf);
		codec.writeString(a);
		codec.writeString(b);
		codec.writeString(c);
		codec.writeString(strangeString);
		String nullA = codec.readString();
		assertEquals(a, nullA);
		assertEquals(b, codec.readString());
		assertEquals(c, codec.readString());
		assertEquals(strangeString, codec.readString());
	}

	@Test
	public void testStringStringMap() {

		// final String strangeString =
		// "{\"1\":{\"str\":\"429bb071\"},\"2\":{\"str\":\"ExchangeTest\"},\"3\":{\"i32\":8},\"4\":{\"str\":\"uft-8\"},\"5\":{\"str\":\"cmessage-adapter 1.0\"},";

		final String input = "{\"1\":{\"str\":\"429bb071\"},"
		      + "\"2\":{\"s\":\"ExchangeTest\"},\"3\":{\"i32\":8},\"4\":{\"str\":\"uft-8\"},"
		      + "\"5\":{\"str\":\"cmessage-adapter 1.0\"},\"6\":{\"i32\":3},\"7\":{\"i32\":1},"
		      + "\"8\":{\"i32\":0},\"9\":{\"str\":\"order_new\"},\"10\":{\"str\":\"\"},"
		      + "\"11\":{\"str\":\"1\"},\"12\":{\"str\":\"DST56615\"},\"13\":{\"str\":\"555555\"},"
		      + "\"14\":{\"str\":\"169.254.142.159\"},\"15\":{\"str\":\"java.lang.String\"},"
		      + "\"16\":{\"i64\":1429168996889},\"17\":{\"map\":[\"str\",\"str\",0,{}]}}";

		Map<String, String> raw = new HashMap<String, String>();
		raw.put(UUID.randomUUID().toString(), input);

		ByteBuf buf = Unpooled.buffer();

		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		codec.writeStringStringMap(raw);

		Map<String, String> decoded = codec.readStringStringMap();

		assertEquals(raw, decoded);
	}

	@Test
	public void testLongIntMap() {
		Map<Long, Integer> raw = new HashMap<Long, Integer>();
		raw.put(100L, 1000);
		raw.put(101L, 1001);

		ByteBuf buf = Unpooled.buffer();

		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		codec.writeLongIntMap(raw);

		Map<Long, Integer> decoded = codec.readLongIntMap();

		assertEquals(raw, decoded);
	}

	@Test
	public void testIntBooleanMap() {
		Map<Integer, Boolean> raw = new HashMap<Integer, Boolean>();
		raw.put(100, true);
		raw.put(101, false);

		ByteBuf buf = Unpooled.buffer();

		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		codec.writeIntBooleanMap(raw);

		Map<Integer, Boolean> decoded = codec.readIntBooleanMap();

		assertEquals(raw, decoded);
	}
}
