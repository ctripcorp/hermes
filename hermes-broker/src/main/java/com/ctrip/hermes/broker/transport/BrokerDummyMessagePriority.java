package com.ctrip.hermes.broker.transport;

import java.io.ByteArrayInputStream;
import java.util.Date;

import com.ctrip.hermes.broker.dal.hermes.MessagePriority;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.transport.DummyMessage;

public class BrokerDummyMessagePriority extends MessagePriority implements DummyMessage {
	public BrokerDummyMessagePriority(Tpp tpp, long id) {
		setId(id);
		setTopic(tpp.getTopic());
		setPartition(tpp.getPartition());
		setPriority(tpp.getPriorityInt());
		setAttributes(DUMMY_HEADER);
		setPayload(new ByteArrayInputStream(new byte[0]));
		setCreationDate(new Date());
	}
}
