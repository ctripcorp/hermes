package com.ctrip.hermes.core.compress;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.zip.GZIPInputStream;

import org.junit.Test;
import org.unidal.helper.Files.AutoClose;
import org.unidal.helper.Files.IO;

public class DeflateTest {

	@Test
	public void testDecompress() throws Exception {
		String file = "/Users/marsqing/tmp/compress/raw.txt";

		// String javacmp = file + ".javacmp";
		// GZIPOutputStream dout = new GZIPOutputStream(new FileOutputStream(javacmp));
		// IO.INSTANCE.copy(new FileInputStream(file), dout, AutoClose.INPUT_OUTPUT);

		String toDecompress = file + ".netcmp";
		GZIPInputStream din = new GZIPInputStream(new FileInputStream(toDecompress));
		IO.INSTANCE.copy(din, new FileOutputStream(toDecompress + ".javadcmp"), AutoClose.INPUT_OUTPUT);
	}

	@Test
	public void testCompress() {

	}

}
