package com.ctrip.hermes.producer.sender;

import java.util.concurrent.Future;

import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.result.SendResult;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface MessageSender {

	Future<SendResult> send(ProducerMessage<?> msg);

}
