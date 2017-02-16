package com.ctrip.hermes.core.transport.command.parser;

import com.ctrip.hermes.core.transport.command.Command;
import com.ctrip.hermes.core.transport.command.Header;

import io.netty.buffer.ByteBuf;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class DefaultCommandParser implements CommandParser {

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ctrip.hermes.remoting.command.CommandParser#parse(io.netty.buffer.ByteBuf)
	 */
	@Override
	public Command parse(ByteBuf frame) {
		Header header = new Header();
		header.parse(frame);

		Class<? extends Command> cmdClazz = header.getType().getClazz();
		Command cmd = null;
		try {
			cmd = cmdClazz.newInstance();
			cmd.parse(frame, header);
			return cmd;
		} catch (Exception e) {
			throw new IllegalArgumentException(String.format("Can not construct command instance for type[%s]", header
			      .getType().getClazz().getName()), e);
		}

	}

}
