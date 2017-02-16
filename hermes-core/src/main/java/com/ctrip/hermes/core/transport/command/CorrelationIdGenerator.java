package com.ctrip.hermes.core.transport.command;

import io.netty.buffer.ByteBuf;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class CorrelationIdGenerator {
	public static long generateCorrelationId() {
		return new DummyCommand().getHeader().getCorrelationId();
	}

	public static class DummyCommand extends AbstractCommand {

		public DummyCommand() {
			super(null, 1);
		}

		private static final long serialVersionUID = 4348425282657231872L;

		@Override
		protected void toBytes0(ByteBuf buf) {
			throw new UnsupportedOperationException();
		}

		@Override
		protected void parse0(ByteBuf buf) {
			throw new UnsupportedOperationException();
		}

	}
}
