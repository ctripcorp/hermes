package com.ctrip.hermes.core.utils;

import io.netty.buffer.ByteBuf;

import java.util.zip.CRC32;

public class ChecksumUtil {

	public static long crc32(ByteBuf buf) {
		CRC32 crc32 = new CRC32();
		
		byte[] dst = new byte[buf.readableBytes()];
		buf.readBytes(dst);
		
		crc32.update(dst);

		// TODO support JDK 8
		// if (buf.isDirect()) {
		// ByteBuffer[] nioBuffers = buf.nioBuffers();
		// for (ByteBuffer nioBuffer : nioBuffers) {
		// // CRC32.update() will update ByteBuffer's position
		// crc32.update(nioBuffer.slice());
		// }
		// } else {
		// crc32.update(buf.array(), buf.arrayOffset(), buf.readableBytes());
		// }

		return crc32.getValue();
	}

}
