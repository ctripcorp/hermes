/**
 * 
 */
package com.ctrip.hermes.core.selector;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import com.ctrip.hermes.core.bo.Tp;
import com.ctrip.hermes.core.selector.Selector.InitialLastUpdateTime;
import com.ctrip.hermes.core.utils.HermesThreadFactory;

/**
 * @author marsqing
 *
 *         Jun 24, 2016 5:50:01 PM
 */
public class PerfTest {

	public static void main(String[] args) throws Exception {
		int slotCount = 3;
		int offsetTtl = 1000;
		int longPollingThreadCount = 50;

		ExecutorService longPollingThreadPool = Executors.newFixedThreadPool(longPollingThreadCount,
		      HermesThreadFactory.create("LongPollingService", true));

		Selector<Tp> s = new DefaultSelector<>(longPollingThreadPool, slotCount, offsetTtl, new OffsetLoader<Tp>() {

			@Override
			public void loadAsync(Tp tp, Selector<Tp> selector) {
				System.out.println("loading");
			}
		}, InitialLastUpdateTime.OLDEST);

		final AtomicLong expireTime = new AtomicLong(Long.MAX_VALUE);
		ExpireTimeHolder expireTimeHolder = new ExpireTimeHolder() {

			@Override
			public long currentExpireTime() {
				return expireTime.get();
			}
		};

		final AtomicLong produceTime = new AtomicLong();
		Tp key = new Tp("topic", 0);
		SelectorCallback callback = new SelectorCallback() {

			@Override
			public void onReady(CallbackContext ctx) {
				System.out.println(System.currentTimeMillis() - produceTime.get());
			}
		};

		for (int i = 1; i < 2; i++) {
			Slot rslot0 = new Slot(0, i);
			Slot rslot1 = new Slot(1, i);
			Slot rslotResend = new Slot(2, System.currentTimeMillis());
			s.register(key, expireTimeHolder, callback, System.currentTimeMillis(), rslot0, rslot1, rslotResend);

			expireTime.set(System.currentTimeMillis() - 1);
			Thread.sleep(2000);
			produceTime.set(System.currentTimeMillis());
			s.register(key, expireTimeHolder, callback, System.currentTimeMillis(), rslot0, rslot1, rslotResend);
			// s.update(key, false, uslot0);
		}

		System.in.read();
	}

}
