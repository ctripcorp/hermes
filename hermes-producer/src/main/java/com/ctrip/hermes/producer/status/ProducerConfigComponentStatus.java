package com.ctrip.hermes.producer.status;

import com.ctrip.framework.vi.annotation.ComponentStatus;
import com.ctrip.framework.vi.annotation.FieldInfo;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.producer.config.ProducerConfig;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@ComponentStatus(id = "hermes.producer.config.component", name = "Hermes Producer Config", description = "Hermes生产者配置")
public class ProducerConfigComponentStatus {

	@FieldInfo(name = "producer.sender.accept.timeout.millis", description = "等待Broker Accept超时(毫秒)")
	private long brokerSenderAcceptTimeoutMillis;

	@FieldInfo(name = "producer.sender.result.timeout.millis", description = "等待Broker发送结果超时(毫秒)")
	private long brokerSenderResultTimeoutMillis;

	@FieldInfo(name = "producer.sender.batchsize", description = "每个partition网络传输最大批量(条)")
	private int brokerSenderBatchSize;

	@FieldInfo(name = "producer.concurrent.level", description = "每个partition网络传输最大并行度")
	private int brokerSenderConcurrentLevel;

	@FieldInfo(name = "producer.sender.taskqueue.size", description = "每个partition的异步发送队列大小")
	private int brokerSenderTaskQueueSize;

	@FieldInfo(name = "producer.callback.threadcount", description = "发送结果回调线程池大小")
	private int producerCallbackThreadCount;

	public ProducerConfigComponentStatus() {
		ProducerConfig config = PlexusComponentLocator.lookup(ProducerConfig.class);

		brokerSenderAcceptTimeoutMillis = config.getBrokerSenderAcceptTimeoutMillis();
		brokerSenderResultTimeoutMillis = config.getBrokerSenderResultTimeoutMillis();
		brokerSenderBatchSize = config.getBrokerSenderBatchSize();
		brokerSenderConcurrentLevel = config.getBrokerSenderConcurrentLevel();
		brokerSenderTaskQueueSize = config.getBrokerSenderTaskQueueSize();
		producerCallbackThreadCount = config.getProducerCallbackThreadCount();
	}

	public long getBrokerSenderAcceptTimeoutMillis() {
		return brokerSenderAcceptTimeoutMillis;
	}

	public long getBrokerSenderResultTimeoutMillis() {
		return brokerSenderResultTimeoutMillis;
	}

	public int getBrokerSenderBatchSize() {
		return brokerSenderBatchSize;
	}

	public int getBrokerSenderConcurrentLevel() {
		return brokerSenderConcurrentLevel;
	}

	public int getBrokerSenderTaskQueueSize() {
		return brokerSenderTaskQueueSize;
	}

	public int getProducerCallbackThreadCount() {
		return producerCallbackThreadCount;
	}

}
