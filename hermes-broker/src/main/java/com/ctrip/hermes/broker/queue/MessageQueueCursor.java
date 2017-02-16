package com.ctrip.hermes.broker.queue;

import java.util.List;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch;
import com.ctrip.hermes.core.selector.CallbackContext;

/**
 * 
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface MessageQueueCursor {

	Pair<Offset, List<TppConsumerMessageBatch>> next(int batchSize, String filter, CallbackContext callbackCtx);

	void init();

	Lease getLease();

	boolean hasError();

	boolean isInited();

	void stop();

}
