package com.ctrip.hermes.broker.queue;

import java.util.List;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch;
import com.ctrip.hermes.core.selector.CallbackContext;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class NoopMessageQueueCursor implements MessageQueueCursor {

	@Override
	public Pair<Offset, List<TppConsumerMessageBatch>> next(int batchSize, String filter, CallbackContext callbackCtx) {
		return null;
	}

	@Override
	public void init() {
		// do nothing
	}

	@Override
	public Lease getLease() {
		return null;
	}

	@Override
	public boolean hasError() {
		return false;
	}

	@Override
	public boolean isInited() {
		return true;
	}

	@Override
	public void stop() {

	}

}
