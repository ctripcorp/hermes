package com.ctrip.hermes.core.status;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import com.ctrip.framework.vi.annotation.ComponentStatus;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@ComponentStatus(id = "hermes.producer.cmd.processor.component", name = "Hermes Cmd Processor Queue", description = "Hermes命令处理器队列")
public class CommandProcessorQueueComponentStatus extends HashMap<String, String> {

	private static final long serialVersionUID = -7336592950789869450L;

	public CommandProcessorQueueComponentStatus() {
		for (Map.Entry<String, BlockingQueue<?>> entry : StatusMonitor.INSTANCE.listCommandProcessorQueues()) {
			this.put(entry.getKey(), Integer.toString(entry.getValue().size()));
		}
	}
}
