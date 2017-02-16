package com.ctrip.hermes.core.transport.netty;

import static org.junit.Assert.assertArrayEquals;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;

import org.junit.Test;

public class MagicTest {

	@Test
	public void testWrite() {
		ByteBuf buf = Unpooled.buffer();
		Magic.writeMagic(buf);

		byte[] bytes = new byte[Magic.MAGIC.length];
		buf.readBytes(bytes);
		assertArrayEquals(Magic.MAGIC, bytes);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testReadAndCheckByteBufFail() {
		byte[] bytes = new byte[Magic.MAGIC.length];
		System.arraycopy(Magic.MAGIC, 0, bytes, 0, Magic.MAGIC.length);
		bytes[bytes.length - 1] = (byte) (bytes[bytes.length - 1] + 1);
		Magic.readAndCheckMagic(Unpooled.wrappedBuffer(bytes));
	}

	@Test
	public void testReadAndCheckByteBuf() {
		byte[] bytes = new byte[Magic.MAGIC.length];
		System.arraycopy(Magic.MAGIC, 0, bytes, 0, Magic.MAGIC.length);
		Magic.readAndCheckMagic(Unpooled.wrappedBuffer(bytes));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testReadAndCheckByteBufferFail() {
		byte[] bytes = new byte[Magic.MAGIC.length];
		System.arraycopy(Magic.MAGIC, 0, bytes, 0, Magic.MAGIC.length);
		bytes[bytes.length - 1] = (byte) (bytes[bytes.length - 1] + 1);
		Magic.readAndCheckMagic(ByteBuffer.wrap(bytes));
	}

	@Test
	public void testReadAndCheckByteBuffer() {
		byte[] bytes = new byte[Magic.MAGIC.length];
		System.arraycopy(Magic.MAGIC, 0, bytes, 0, Magic.MAGIC.length);
		Magic.readAndCheckMagic(ByteBuffer.wrap(bytes));
	}

}
