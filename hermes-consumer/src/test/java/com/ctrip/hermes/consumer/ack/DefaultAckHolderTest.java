package com.ctrip.hermes.consumer.ack;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.ctrip.hermes.consumer.engine.ack.AckHolderScanningResult;
import com.ctrip.hermes.consumer.engine.ack.DefaultAckHolder;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class DefaultAckHolderTest {

	private DefaultAckHolder<String> ackHolder;

	@Before
	public void before() throws Exception {
		ackHolder = new DefaultAckHolder<>("test-holder", 50);
	}

	@Test
	public void testContinousAckOrNack() throws Exception {
		int count = 50;
		for (int i = 0; i < count; i++) {
			ackHolder.delivered(i, String.valueOf(i));
		}

		for (int i = 0; i < count; i++) {
			if (i % 2 == 0) {
				ackHolder.ack(i, null);
			} else {
				ackHolder.nack(i, null);
			}
		}

		AckHolderScanningResult<String> res = ackHolder.scan(10000);

		List<String> acked = res.getAcked();
		List<String> nacked = res.getNacked();

		assertEquals(count / 2, acked.size());
		assertEquals(count / 2, nacked.size());
		for (int i = 0; i < count; i++) {
			if (i % 2 == 0) {
				assertTrue(acked.contains(String.valueOf(i)));
			} else {
				assertTrue(nacked.contains(String.valueOf(i)));
			}
		}
	}

	@Test
	public void testWithNoAckAndNack() throws Exception {
		int count = 55;
		for (int i = 0; i < count; i++) {
			ackHolder.delivered(i, String.valueOf(i));
		}

		ackHolder.ack(0, null);
		ackHolder.ack(1, null);
		ackHolder.ack(2, null);
		ackHolder.ack(6, null);

		ackHolder.nack(3, null);
		ackHolder.nack(4, null);
		ackHolder.nack(7, null);

		AckHolderScanningResult<String> res = ackHolder.scan(10000);
		List<String> acked = res.getAcked();
		List<String> nacked = res.getNacked();

		assertEquals(3, acked.size());
		assertEquals(2, nacked.size());

		assertTrue(acked.contains("0"));
		assertTrue(acked.contains("1"));
		assertTrue(acked.contains("2"));

		assertTrue(nacked.contains("3"));
		assertTrue(nacked.contains("4"));
	}

	@Test
	public void testWithNoAckAndNack2() throws Exception {
		int count = 60;
		for (int i = 0; i < count; i++) {
			ackHolder.delivered(i, String.valueOf(i));
		}

		ackHolder.ack(0, null);
		ackHolder.ack(1, null);
		ackHolder.ack(2, null);
		ackHolder.ack(6, null);

		ackHolder.nack(3, null);
		ackHolder.nack(4, null);
		ackHolder.nack(7, null);

		AckHolderScanningResult<String> res = ackHolder.scan(10000);
		List<String> acked = res.getAcked();
		List<String> nacked = res.getNacked();

		assertEquals(4, acked.size());
		assertEquals(6, nacked.size());

		assertTrue(acked.contains("0"));
		assertTrue(acked.contains("1"));
		assertTrue(acked.contains("2"));
		assertTrue(acked.contains("6"));

		assertTrue(nacked.contains("3"));
		assertTrue(nacked.contains("4"));
		assertTrue(nacked.contains("5"));
		assertTrue(nacked.contains("7"));
		assertTrue(nacked.contains("8"));
		assertTrue(nacked.contains("9"));
	}
}
