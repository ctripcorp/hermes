package com.ctrip.hermes.consumer.engine.ack;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.transport.command.v5.AckMessageCommandV5;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface AckManager {
	public void register(long token, Tpg tpg, int maxAckHolderSize);

	public void ack(long token, ConsumerMessage<?> msg);

	public void nack(long token, ConsumerMessage<?> msg);

	public void delivered(long token, ConsumerMessage<?> msg);

	public void deregister(long token);

	boolean writeAckToBroker(AckMessageCommandV5 cmd);
}
