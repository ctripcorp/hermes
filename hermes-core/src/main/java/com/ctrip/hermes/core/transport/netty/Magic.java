package com.ctrip.hermes.core.transport.netty;

import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;

public class Magic {

	final static byte[] MAGIC = new byte[] { 'h', 'e', 'm', 's' };

	public static void readAndCheckMagic(ByteBuffer buf) {
		byte[] magic = new byte[MAGIC.length];
		buf.get(magic);
		for (int i = 0; i < magic.length; i++) {
			if (magic[i] != MAGIC[i]) {
				throw new IllegalArgumentException("Magic number mismatch");
			}
		}
	}

	public static void readAndCheckMagic(ByteBuf buf) {
		byte[] magic = new byte[MAGIC.length];
		buf.readBytes(magic);
		for (int i = 0; i < magic.length; i++) {
			if (magic[i] != MAGIC[i]) {
				throw new IllegalArgumentException("Magic number mismatch");
			}
		}
	}

	public static void writeMagic(ByteBuf buf) {
		buf.writeBytes(MAGIC);
	}

	public static int length() {
		return MAGIC.length;
	}

}
