package com.ctrip.hermes.producer.sender;

import java.util.concurrent.Future;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.message.partition.PartitioningStrategy;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.core.transport.endpoint.EndpointClient;
import com.ctrip.hermes.core.transport.endpoint.EndpointManager;
import com.ctrip.hermes.env.ClientEnvironment;
import com.ctrip.hermes.producer.monitor.SendMessageAcceptanceMonitor;
import com.ctrip.hermes.producer.monitor.SendMessageResultMonitor;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public abstract class AbstractMessageSender implements MessageSender {

	private static final String PK = "pK";

	@Inject
	protected EndpointManager m_endpointManager;

	@Inject
	protected EndpointClient m_endpointClient;

	@Inject
	protected PartitioningStrategy m_partitioningAlgo;

	@Inject
	protected MetaService m_metaService;

	@Inject
	protected SendMessageAcceptanceMonitor m_messageAcceptanceMonitor;

	@Inject
	protected SendMessageResultMonitor m_messageResultMonitor;
	
	@Inject
	protected ClientEnvironment m_clientEnv;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ctrip.hermes.message.internal.MessageSender#send(com.ctrip.hermes.message.ProducerMessage)
	 */
	@Override
	public Future<SendResult> send(ProducerMessage<?> msg) {
		preSend(msg);
		return doSend(msg);
	}

	protected abstract Future<SendResult> doSend(ProducerMessage<?> msg);

	protected void preSend(ProducerMessage<?> msg) {
		if (msg.getPartitionKey() != null) {
			msg.addDurableSysProperty(PK, String.valueOf(msg.getPartitionKey().hashCode()));
		}
		int partitionNo = m_partitioningAlgo.computePartitionNo(msg.getPartitionKey(), m_metaService
		      .listPartitionsByTopic(msg.getTopic()).size());
		msg.setPartition(partitionNo);
	}

	protected int getPartitionKeyHashCode(ProducerMessage<?> msg) {
		return Integer.parseInt(msg.getDurableSysProperty(PK));
	}
}
