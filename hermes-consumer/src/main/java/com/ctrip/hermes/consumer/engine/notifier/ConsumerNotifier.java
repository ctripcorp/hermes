package com.ctrip.hermes.consumer.engine.notifier;

import java.util.List;

import com.ctrip.hermes.consumer.engine.ConsumerContext;
import com.ctrip.hermes.core.message.ConsumerMessage;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface ConsumerNotifier {

	void register(long token, ConsumerContext consumerContext);

	void deregister(long token, boolean force);

	void messageReceived(long token, List<ConsumerMessage<?>> msgs);

	ConsumerContext find(long token);

}
