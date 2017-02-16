package com.ctrip.hermes.core.transport;

public interface DummyMessage {
	public static final String DUMMY_HEADER_KEY = "SYS.FILTER";

	public static final String DUMMY_HEADER_VALUE = "EMPTY";

	public static final byte[] DUMMY_HEADER = new byte[] { 0, 0, 0, 1, //
	      0, 0, 0, 10, //
	      'S', 'Y', 'S', '.', 'F', 'I', 'L', 'T', 'E', 'R', //
	      0, 0, 0, 5, //
	      'E', 'M', 'P', 'T', 'Y' };
}
