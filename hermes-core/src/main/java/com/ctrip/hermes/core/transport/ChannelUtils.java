package com.ctrip.hermes.core.transport;

import io.netty.channel.Channel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.core.transport.netty.NettyUtils;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class ChannelUtils {
	private static final Logger log = LoggerFactory.getLogger(ChannelUtils.class);

	public static boolean writeAndFlush(Channel channel, Object msg) {
		if (channel.isActive() && channel.isWritable()) {
			channel.writeAndFlush(msg);
			return true;
		} else {
			log.warn("WriteAndFlush failed to {}(active:{}, writable:{})", NettyUtils.parseChannelRemoteAddr(channel),
			      channel.isActive(), channel.isWritable());
			return false;
		}
	}
}
