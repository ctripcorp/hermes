package com.ctrip.hermes.core.selector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.junit.Test;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.selector.Selector.InitialLastUpdateTime;
import com.google.common.util.concurrent.MoreExecutors;

public class SelectorTest {

	private Selector<String> s;

	private ExpireTimeHolder expireTimeHolder = new ExpireTimeHolder() {

		@Override
		public long currentExpireTime() {
			return Long.MAX_VALUE;
		}
	};

	private Selector<String> makeSelector(int slotCount, InitialLastUpdateTime initialLastUpdateTime) {
		return new DefaultSelector<>(MoreExecutors.newDirectExecutorService(), slotCount, 5000,
		      new OffsetLoader<String>() {

			      @Override
			      public void loadAsync(String key, Selector<String> selector) {
			      }
		      }, initialLastUpdateTime);
	}

	@Before
	public void before() {
		int slotCount = 3;
		s = makeSelector(slotCount, InitialLastUpdateTime.OLDEST);
	}

	@Test
	public void testFirstRegisterFire() throws Exception {
		String key = "";
		Slot slot = new Slot(0, 1);

		final CountDownLatch latch = new CountDownLatch(1);
		s.register(key, expireTimeHolder, new SelectorCallback() {

			@Override
			public void onReady(CallbackContext ctx) {
				latch.countDown();
			}
		}, System.currentTimeMillis(), slot);

		assertTrue(latch.await(100, TimeUnit.MILLISECONDS));
	}

	@Test
	public void testUpdateRegisterFire() throws Exception {
		String key = "";
		Slot slot = new Slot(0, 1);
		s.update(key, true, slot);

		final CountDownLatch latch = new CountDownLatch(1);
		s.register(key, expireTimeHolder, new SelectorCallback() {

			@Override
			public void onReady(CallbackContext ctx) {
				latch.countDown();
			}
		}, System.currentTimeMillis(), slot);

		assertTrue(latch.await(200, TimeUnit.MILLISECONDS));
	}

	@Test
	public void testUpdateRegisterNotFire() throws Exception {
		String key = "";
		Slot slot = new Slot(0, 1);
		s.update(key, true, slot);

		final CountDownLatch latch = new CountDownLatch(1);
		s.register(key, expireTimeHolder, new SelectorCallback() {

			@Override
			public void onReady(CallbackContext ctx) {
				latch.countDown();
			}
		}, System.currentTimeMillis(), new Slot(0, 2));

		assertFalse(latch.await(200, TimeUnit.MILLISECONDS));
	}

	@Test
	public void testRegisterUpdateFire() throws Exception {
		String key = "";
		Slot slot = new Slot(0, 1);
		final CountDownLatch latch = new CountDownLatch(1);
		s.register(key, expireTimeHolder, new SelectorCallback() {

			@Override
			public void onReady(CallbackContext ctx) {
				latch.countDown();
			}
		}, System.currentTimeMillis(), slot);

		s.update(key, true, slot);

		assertTrue(latch.await(200, TimeUnit.MILLISECONDS));
	}

	@Test
	public void testRegisterUpdateNotFire() throws Exception {
		int slotCount = 3;
		s = new DefaultSelector<>(MoreExecutors.newDirectExecutorService(), slotCount, 5000, new OffsetLoader<String>() {

			@Override
			public void loadAsync(String key, Selector<String> selector) {
			}
		}, InitialLastUpdateTime.NEWEST);

		String key = "";
		Slot slot = new Slot(0, 2);
		final CountDownLatch latch = new CountDownLatch(1);
		s.register(key, expireTimeHolder, new SelectorCallback() {

			@Override
			public void onReady(CallbackContext ctx) {
				latch.countDown();
			}
		}, System.currentTimeMillis(), slot);

		s.update(key, true, new Slot(0, 1));

		assertFalse(latch.await(200, TimeUnit.MILLISECONDS));
	}

	@Test
	public void testMultipleUpdate() throws Exception {
		String key = "";
		Slot slot = new Slot(0, 1);
		final AtomicInteger called = new AtomicInteger();
		final CountDownLatch latch = new CountDownLatch(1);
		s.register(key, expireTimeHolder, new SelectorCallback() {

			@Override
			public void onReady(CallbackContext ctx) {
				called.incrementAndGet();
				latch.countDown();
			}
		}, System.currentTimeMillis(), slot);

		s.update(key, true, new Slot(0, 1));
		s.update(key, true, new Slot(0, 2));

		assertTrue(latch.await(200, TimeUnit.MILLISECONDS));
		assertEquals(1, called.get());
	}

	@Test
	public void testRegisterMultipleSameSlot() throws Exception {
		String key = "";
		Slot slot = new Slot(0, 1);

		final CountDownLatch latch1 = new CountDownLatch(1);
		s.register(key, expireTimeHolder, new SelectorCallback() {

			@Override
			public void onReady(CallbackContext ctx) {
				latch1.countDown();
			}
		}, System.currentTimeMillis(), slot);

		final CountDownLatch latch2 = new CountDownLatch(1);
		s.register(key, expireTimeHolder, new SelectorCallback() {

			@Override
			public void onReady(CallbackContext ctx) {
				latch2.countDown();
			}
		}, System.currentTimeMillis(), new Slot(0, 2));

		s.update(key, true, new Slot(0, 2));

		assertTrue(latch1.await(200, TimeUnit.MILLISECONDS));
		assertTrue(latch2.await(200, TimeUnit.MILLISECONDS));
	}

	@Test
	public void testRegisterMultipleDiffSlot() throws Exception {
		String key = "";
		final AtomicInteger called = new AtomicInteger();

		final CountDownLatch latch1 = new CountDownLatch(1);
		s.register(key, expireTimeHolder, new SelectorCallback() {

			@Override
			public void onReady(CallbackContext ctx) {
				called.incrementAndGet();
				latch1.countDown();
			}
		}, System.currentTimeMillis(), new Slot(0, 2L));

		final CountDownLatch latch2 = new CountDownLatch(1);
		s.register(key, expireTimeHolder, new SelectorCallback() {

			@Override
			public void onReady(CallbackContext ctx) {
				called.incrementAndGet();
				latch2.countDown();
			}
		}, System.currentTimeMillis(), new Slot(1, 20L));

		s.update(key, true, new Slot(0, 2));

		assertTrue(latch1.await(200, TimeUnit.MILLISECONDS));
		assertFalse(latch2.await(200, TimeUnit.MILLISECONDS));

		s.update(key, true, new Slot(1, 20));
		assertTrue(latch2.await(200, TimeUnit.MILLISECONDS));
		assertEquals(2, called.get());
	}

	@Test
	public void testResend() throws Exception {
		int slotCount = 3;
		Selector<Pair<String, Integer>> s = new DefaultSelector<>(MoreExecutors.newDirectExecutorService(), slotCount,
		      5000, new OffsetLoader<Pair<String, Integer>>() {
			      @Override
			      public void loadAsync(Pair<String, Integer> key, Selector<Pair<String, Integer>> selector) {
			      }
		      }, InitialLastUpdateTime.OLDEST);

		int partition = 0;
		Pair<String, Integer> key = new Pair<>("topic", partition);
		Slot slotNonPriority = new Slot(0, 10L);
		Slot slotPriority = new Slot(1, 20L);
		Slot slotResend = new Slot(2, System.currentTimeMillis());

		for (int i = 0; i < slotCount; i++) {
			final CountDownLatch latch = new CountDownLatch(1);
			s.register(key, expireTimeHolder, new SelectorCallback() {

				@Override
				public void onReady(CallbackContext ctx) {
					latch.countDown();
				}
			}, System.currentTimeMillis(), slotNonPriority, slotPriority, slotResend);

			long offset = i == slotCount - 1 ? System.currentTimeMillis() : (i + 1) * 10;
			s.update(key, true, new Slot(i, offset));

			assertTrue(latch.await(200, TimeUnit.MILLISECONDS));
		}
	}

	@Test
	public void testLongTimeNoUpdate() throws Exception {
		final AtomicInteger loadAsyncCalled = new AtomicInteger();
		int ttl = -1;
		s = new DefaultSelector<>(MoreExecutors.newDirectExecutorService(), 3, ttl, new OffsetLoader<String>() {

			@Override
			public void loadAsync(String key, Selector<String> selector) {
				loadAsyncCalled.incrementAndGet();
			}
		}, InitialLastUpdateTime.OLDEST);

		String key = "";
		s.update(key, true, new Slot(0, 1));

		final CountDownLatch latch = new CountDownLatch(1);
		s.register(key, expireTimeHolder, new SelectorCallback() {

			@Override
			public void onReady(CallbackContext ctx) {
				latch.countDown();
			}
		}, System.currentTimeMillis(), new Slot(0, 10));

		assertTrue(latch.await(100, TimeUnit.MILLISECONDS));
		assertEquals(1, loadAsyncCalled.get());
	}

	@Test
	public void testUpdateTriggedCallbackContext() throws Exception {
		s = makeSelector(3, InitialLastUpdateTime.NEWEST);

		String key = "";

		final CountDownLatch latch = new CountDownLatch(1);
		final AtomicReference<CallbackContext> ctxRef = new AtomicReference<CallbackContext>();
		s.register(key, expireTimeHolder, new SelectorCallback() {

			@Override
			public void onReady(CallbackContext ctx) {
				ctxRef.set(ctx);
				latch.countDown();
			}
		}, System.currentTimeMillis(), new Slot(0, 10), new Slot(1, 20), new Slot(2, 30));

		s.update(key, true, new Slot(1, 22));

		assertTrue(latch.await(100, TimeUnit.MILLISECONDS));
		SlotMatchResult[] actualResults = ctxRef.get().getSlotMatchResults();
		SlotMatchResult[] expResults = new SlotMatchResult[] { //
		new SlotMatchResult(0, false, 10, -1), //
		      new SlotMatchResult(1, true, 20, 22), //
		      new SlotMatchResult(2, false, 30, -1) };

		for (int i = 0; i < actualResults.length; i++) {
			assertEquals(expResults[i], actualResults[i]);
		}
	}

	@Test
	public void testRegisterTriggedCallbackContext() throws Exception {
		s = makeSelector(3, InitialLastUpdateTime.NEWEST);

		String key = "";

		s.update(key, true, new Slot(0, 1), new Slot(1, 2), new Slot(2, 3));

		final CountDownLatch latch = new CountDownLatch(1);
		final AtomicInteger called = new AtomicInteger(0);
		final AtomicReference<CallbackContext> ctxRef = new AtomicReference<CallbackContext>();
		s.register(key, expireTimeHolder, new SelectorCallback() {

			@Override
			public void onReady(CallbackContext ctx) {
				called.incrementAndGet();
				ctxRef.set(ctx);
				latch.countDown();
			}
		}, System.currentTimeMillis(), new Slot(0, 10), new Slot(1, 20), new Slot(2, 30));

		s.update(key, true, new Slot(0, 11), new Slot(1, 22));

		assertTrue(latch.await(100, TimeUnit.MILLISECONDS));
		assertEquals(1, called.get());
		SlotMatchResult[] actualResults = ctxRef.get().getSlotMatchResults();
		SlotMatchResult[] expResults = new SlotMatchResult[] { //
		new SlotMatchResult(0, true, 10, 11), //
		      new SlotMatchResult(1, true, 20, 22), //
		      new SlotMatchResult(2, false, 30, 3) };

		for (int i = 0; i < actualResults.length; i++) {
			assertEquals(expResults[i], actualResults[i]);
		}
	}
}
