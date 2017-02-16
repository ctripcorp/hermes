package com.ctrip.hermes.core.transport;

import io.netty.buffer.ByteBuf;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface TransferCallback {

	public void transfer(ByteBuf out);

}
