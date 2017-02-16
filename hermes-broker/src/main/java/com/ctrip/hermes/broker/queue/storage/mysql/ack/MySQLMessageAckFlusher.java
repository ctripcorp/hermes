package com.ctrip.hermes.broker.queue.storage.mysql.ack;

import com.ctrip.hermes.broker.dal.hermes.OffsetMessage;
import com.ctrip.hermes.broker.dal.hermes.OffsetResend;

public interface MySQLMessageAckFlusher {
	public void ackOffsetMessage(OffsetMessage offsetMessage);

	public void ackOffsetResend(OffsetResend offsetResend);
}
