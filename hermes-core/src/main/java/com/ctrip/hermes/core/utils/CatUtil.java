package com.ctrip.hermes.core.utils;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.unidal.tuple.Pair;

import com.dianping.cat.Cat;
import com.dianping.cat.message.Event;
import com.dianping.cat.message.Transaction;
import com.dianping.cat.message.internal.DefaultTransaction;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class CatUtil {

	private static LoadingCache<Pair<String, String>, Pair<AtomicLong, AtomicLong>> m_catEventLastLogTimes;

	static {
		m_catEventLastLogTimes = CacheBuilder.newBuilder().maximumSize(10000)
		      .build(new CacheLoader<Pair<String, String>, Pair<AtomicLong, AtomicLong>>() {

			      @Override
			      public Pair<AtomicLong, AtomicLong> load(Pair<String, String> key) throws Exception {
				      return new Pair<>(new AtomicLong(0), new AtomicLong(System.currentTimeMillis()));
			      }
		      });
	}

	public static void logElapse(String type, String name, long startTimestamp, int count,
	      List<Pair<String, String>> datas, String status) {
		if (count > 0) {
			Transaction latencyT = Cat.newTransaction(type, name);
			long delta = System.currentTimeMillis() - startTimestamp;

			if (latencyT instanceof DefaultTransaction) {
				((DefaultTransaction) latencyT).setDurationStart(System.nanoTime() - delta * 1000000L);
			}
			latencyT.addData("*count", count);
			if (datas != null && !datas.isEmpty()) {
				for (Pair<String, String> data : datas) {
					latencyT.addData(data.getKey(), data.getValue());
				}
			}
			latencyT.setStatus(status);
			latencyT.complete();
		}
	}

	public static void logEventPeriodically(String type, String name) {
		logEventPeriodically(type, name, 1L);
	}

	public static void logEventPeriodically(String type, String name, long count) {
		Pair<AtomicLong, AtomicLong> countAndLastLogTimePair = m_catEventLastLogTimes
		      .getUnchecked(new Pair<>(type, name));
		long now = System.currentTimeMillis();

		if (now - countAndLastLogTimePair.getValue().get() >= 20000) {
			countAndLastLogTimePair.getValue().set(now);
			Cat.logEvent(type, name, Event.SUCCESS, "*count=" + countAndLastLogTimePair.getKey().getAndSet(0));
		} else {
			countAndLastLogTimePair.getKey().addAndGet(count);
		}
	}
}
