package com.ctrip.hermes.broker.ack.internal;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.broker.ack.internal.BatchResult;
import com.ctrip.hermes.broker.ack.internal.ContinuousRange;
import com.ctrip.hermes.broker.ack.internal.DefaultAckHolder;
import com.ctrip.hermes.broker.ack.internal.EnumRange;

public class DefaultAckHolderTest {

	private DefaultAckHolder<String> m;

	@Before
	public void before() {
		m = new DefaultAckHolder<String>(5000);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testSingleSuccess() throws Exception {
		String expId = uuid();
		check(m, expId, 1, Arrays.asList(0), Collections.EMPTY_LIST, new int[] { 0, 0 }, new int[0]);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testSingleFail() throws Exception {
		String expId = uuid();
		check(m, expId, 1, Collections.EMPTY_LIST, Arrays.asList(0), new int[] { 0, 0 }, new int[] { 0 });
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testSingleTimeout() throws Exception {
		m = new DefaultAckHolder<String>(10) {

			@Override
			protected boolean isTimeout(long start, int timeout) {
				return true;
			}

		};
		String expId = uuid();
		check(m, expId, 1, Collections.EMPTY_LIST, Collections.EMPTY_LIST, new int[] { 0, 0 }, new int[] { 0 });
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testMixed1() throws Exception {
		m = new DefaultAckHolder<String>(10) {

			@Override
			protected boolean isTimeout(long start, int timeout) {
				return true;
			}

		};
		String expId = uuid();
		check(m, expId, 10, Arrays.asList(0, 1, 2, 9), Collections.EMPTY_LIST, //
		      new int[] { 0, 9 }, new int[] { 3, 4, 5, 6, 7, 8 });
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testMixed2() throws Exception {
		m = new DefaultAckHolder<String>(10) {

			@Override
			protected boolean isTimeout(long start, int timeout) {
				return true;
			}

		};
		String expId = uuid();
		check(m, expId, 10, Arrays.asList(1, 5, 8), Collections.EMPTY_LIST, //
		      new int[] { 0, 9 }, new int[] { 0, 2, 3, 4, 6, 7, 9 });
	}

	@Test
	public void testMixed3() throws Exception {
		m = new DefaultAckHolder<String>(10) {

			@Override
			protected boolean isTimeout(long start, int timeout) {
				return true;
			}

		};
		String expId = uuid();
		check(m, expId, 10, Arrays.asList(1, 5, 8), Arrays.asList(2, 6, 9), //
		      new int[] { 0, 9 }, new int[] { 0, 2, 3, 4, 6, 7, 9 });
	}

	private void check(DefaultAckHolder<String> m, //
	      final String expId, int totalOffsets, //
	      List<Integer> successes, List<Integer> fails, //
	      final int[] successIdxes, final int[] failIdxes) throws Exception {
		// offsets
		final List<Long> offsets = new ArrayList<>();
		Random rnd = new Random(System.currentTimeMillis());
		for (int i = 0; i < totalOffsets; i++) {
			long offset = rnd.nextLong();
			offsets.add(offset);
		}
		Collections.sort(offsets);

		// delivered
		final EnumRange<String> allDelivered = new EnumRange<>();
		final Map<Long, String> ctxMap = new HashMap<>();
		for (Long offset : offsets) {
			String ctx = uuid();
			ctxMap.put(offset, ctx);
			allDelivered.addOffset(offset, ctx);

			// test merge
			m.delivered(Arrays.asList(new Pair<>(offset, ctx)), System.currentTimeMillis());
		}

		// ack
		for (Integer success : successes) {
			m.acked(allDelivered.getOffsets().get(success).getKey(), true);
		}
		for (Integer fail : fails) {
			m.acked(allDelivered.getOffsets().get(fail).getKey(), false);
		}

		BatchResult<String> batchResult = m.scan();
		ContinuousRange doneRange = batchResult.getDoneRange();
		EnumRange<String> failRange = batchResult.getFailRange();

		System.out.println("Range done: " + doneRange);
		assertEquals(new ContinuousRange(offsets.get(successIdxes[0]), offsets.get(successIdxes[1])), doneRange);

		if (failIdxes.length > 0) {
			System.out.println("Range fail: " + failRange);

			EnumRange<String> expRange = new EnumRange<>();
			for (Integer failIdx : failIdxes) {
				long offset = offsets.get(failIdx);
				expRange.addOffset(offset, ctxMap.get(offset));
			}
			assertEquals(expRange, failRange);
		}

	}

	private String uuid() {
		return UUID.randomUUID().toString();
	}

}
