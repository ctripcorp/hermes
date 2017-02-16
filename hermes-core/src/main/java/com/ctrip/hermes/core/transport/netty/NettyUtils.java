package com.ctrip.hermes.core.transport.netty;

import io.netty.channel.Channel;

import java.net.SocketAddress;

import com.ctrip.hermes.core.utils.StringUtils;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class NettyUtils {
	public static String parseChannelRemoteAddr(final Channel channel) {
		return parseChannelRemoteAddr(channel, true);
	}

	public static String parseChannelRemoteAddr(final Channel channel, boolean withPort) {
		String ipPort = parseIpPort(channel);

		if (withPort) {
			return ipPort;
		} else {
			return stripPort(ipPort);
		}
	}

	private static String stripPort(String ipPort) {
		if (StringUtils.isEmpty(ipPort)) {
			return ipPort;
		} else {
			int idxColon = ipPort.indexOf(":");
			if (idxColon > 0) {
				return ipPort.substring(0, idxColon);
			} else {
				return ipPort;
			}
		}
	}

	private static String parseIpPort(final Channel channel) {
		if (null == channel) {
			return "";
		}
		final SocketAddress remote = channel.remoteAddress();
		final String addr = remote != null ? remote.toString() : "";

		if (addr.length() > 0) {
			int index = addr.lastIndexOf("/");
			if (index >= 0) {
				return addr.substring(index + 1);
			}

			return addr;
		}

		return "";
	}
}
