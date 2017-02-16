package com.ctrip.hermes.core.message.payload;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.unidal.helper.Files.IO;

public class PayloadCodecFactoryTest {

	@Test
	public void test() throws Exception {
		PayloadCodec jsonGzipCodec = PayloadCodecFactory.getCodecByType("json,gzip");
		PayloadCodec jsonCodec = PayloadCodecFactory.getCodecByType("json");
		assertTrue(jsonGzipCodec instanceof PayloadCodecCompositor);
		String xml = IO.INSTANCE.readFrom(this.getClass().getResourceAsStream("/META-INF/dal/model/meta-codegen.xml"),
		      "utf-8");

		byte[] gzipEncoded = jsonGzipCodec.encode("topic", xml);
		byte[] encoded = jsonCodec.encode("topic", xml);
		assertTrue(gzipEncoded.length < encoded.length);

		String xmlDecodec = jsonGzipCodec.decode(gzipEncoded, String.class);
		assertEquals(xml, xmlDecodec);
	}

}
