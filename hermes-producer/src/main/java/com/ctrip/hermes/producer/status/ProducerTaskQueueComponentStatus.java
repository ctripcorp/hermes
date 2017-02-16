package com.ctrip.hermes.producer.status;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.unidal.tuple.Pair;

import com.ctrip.framework.vi.annotation.ComponentStatus;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@ComponentStatus(id = "hermes.producer.taskqueue.component", name = "Hermes Producer Send Task Queue", description = "Hermes生产者后台异步发送队列状态")
public class ProducerTaskQueueComponentStatus extends HashMap<String, String> {

	private static final long serialVersionUID = 1904630475810790200L;

	public ProducerTaskQueueComponentStatus() {
		for (Map.Entry<Pair<String, Integer>, BlockingQueue<?>> entry : ProducerStatusMonitor.INSTANCE.listTaskQueues()) {
			Pair<String, Integer> tp = entry.getKey();
			this.put(tp.getKey() + "-" + tp.getValue(), Integer.toString(entry.getValue().size()));
		}
	}
}
