package com.ctrip.hermes.consumer.engine.status;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.unidal.tuple.Triple;

import com.ctrip.framework.vi.annotation.ComponentStatus;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@ComponentStatus(id = "hermes.producer.localcache.component", name = "Hermes Consumer Local Cache", description = "Hermes消费者本地缓存状态")
public class ConsumerLocalCacheComponentStatus extends HashMap<String, String> {

	private static final long serialVersionUID = 1904630475810790200L;

	public ConsumerLocalCacheComponentStatus() {
		for (Map.Entry<Triple<String, Integer, String>, BlockingQueue<?>> entry : ConsumerStatusMonitor.INSTANCE
		      .listLocalCaches()) {
			Triple<String, Integer, String> tpg = entry.getKey();
			this.put(tpg.getFirst() + "-" + tpg.getMiddle() + ":" + tpg.getLast(),
			      Integer.toString(entry.getValue().size()));
		}
	}
}
