package com.ctrip.hermes.core.transport.command.processor;

import io.netty.channel.Channel;

import com.ctrip.hermes.core.transport.command.Command;
import com.ctrip.hermes.core.transport.netty.NettyUtils;

public class CommandProcessorContext {

	private Command m_command;

	private Channel m_channel;

	private String m_remoteIp;

	public CommandProcessorContext(Command command, Channel channel) {
		m_command = command;
		m_channel = channel;
		m_remoteIp = NettyUtils.parseChannelRemoteAddr(channel, false);
	}

	public Command getCommand() {
		return m_command;
	}

	public Channel getChannel() {
		return m_channel;
	}

	public String getRemoteIp() {
		return m_remoteIp;
	}

}
