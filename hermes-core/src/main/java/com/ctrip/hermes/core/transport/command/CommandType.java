package com.ctrip.hermes.core.transport.command;

import java.util.HashMap;
import java.util.Map;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.transport.command.v2.AckMessageCommandV2;
import com.ctrip.hermes.core.transport.command.v2.PullMessageCommandV2;
import com.ctrip.hermes.core.transport.command.v2.PullMessageResultCommandV2;
import com.ctrip.hermes.core.transport.command.v2.PullSpecificMessageCommand;
import com.ctrip.hermes.core.transport.command.v3.AckMessageCommandV3;
import com.ctrip.hermes.core.transport.command.v3.AckMessageResultCommandV3;
import com.ctrip.hermes.core.transport.command.v3.PullMessageCommandV3;
import com.ctrip.hermes.core.transport.command.v3.PullMessageResultCommandV3;
import com.ctrip.hermes.core.transport.command.v3.QueryLatestConsumerOffsetCommandV3;
import com.ctrip.hermes.core.transport.command.v3.QueryOffsetResultCommandV3;
import com.ctrip.hermes.core.transport.command.v3.SendMessageCommandV3;
import com.ctrip.hermes.core.transport.command.v4.AckMessageCommandV4;
import com.ctrip.hermes.core.transport.command.v4.PullMessageCommandV4;
import com.ctrip.hermes.core.transport.command.v4.PullMessageResultCommandV4;
import com.ctrip.hermes.core.transport.command.v5.AckMessageAckCommandV5;
import com.ctrip.hermes.core.transport.command.v5.AckMessageCommandV5;
import com.ctrip.hermes.core.transport.command.v5.AckMessageResultCommandV5;
import com.ctrip.hermes.core.transport.command.v5.PullMessageAckCommandV5;
import com.ctrip.hermes.core.transport.command.v5.PullMessageCommandV5;
import com.ctrip.hermes.core.transport.command.v5.PullMessageResultCommandV5;
import com.ctrip.hermes.core.transport.command.v5.QueryLatestConsumerOffsetAckCommandV5;
import com.ctrip.hermes.core.transport.command.v5.QueryLatestConsumerOffsetCommandV5;
import com.ctrip.hermes.core.transport.command.v5.QueryOffsetResultCommandV5;
import com.ctrip.hermes.core.transport.command.v5.SendMessageAckCommandV5;
import com.ctrip.hermes.core.transport.command.v5.SendMessageCommandV5;
import com.ctrip.hermes.core.transport.command.v6.FetchManualConfigCommandV6;
import com.ctrip.hermes.core.transport.command.v6.FetchManualConfigResultCommandV6;
import com.ctrip.hermes.core.transport.command.v6.SendMessageAckCommandV6;
import com.ctrip.hermes.core.transport.command.v6.SendMessageCommandV6;
import com.ctrip.hermes.core.transport.command.v6.SendMessageResultCommandV6;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public enum CommandType {
	MESSAGE_SEND(101, 1, SendMessageCommand.class), //
	MESSAGE_SEND_V3(101, 3, SendMessageCommandV3.class), //
	MESSAGE_SEND_V5(101, 5, SendMessageCommandV5.class), //
	MESSAGE_SEND_V6(101, 6, SendMessageCommandV6.class), //

	MESSAGE_ACK(102, 1, AckMessageCommand.class), //
	MESSAGE_ACK_V2(102, 2, AckMessageCommandV2.class), //
	MESSAGE_ACK_V3(102, 3, AckMessageCommandV3.class), //
	MESSAGE_ACK_V4(102, 4, AckMessageCommandV4.class), //
	MESSAGE_ACK_V5(102, 5, AckMessageCommandV5.class), //

	MESSAGE_PULL(103, 1, PullMessageCommand.class), //
	MESSAGE_PULL_V2(103, 2, PullMessageCommandV2.class), //
	MESSAGE_PULL_V3(103, 3, PullMessageCommandV3.class), //
	MESSAGE_PULL_V4(103, 4, PullMessageCommandV4.class), //
	MESSAGE_PULL_V5(103, 5, PullMessageCommandV5.class), //

	QUERY_LATEST_CONSUMER_OFFSET(104, 1, QueryLatestConsumerOffsetCommand.class), //
	QUERY_LATEST_CONSUMER_OFFSET_V3(104, 3, QueryLatestConsumerOffsetCommandV3.class), //
	QUERY_LATEST_CONSUMER_OFFSET_V5(104, 5, QueryLatestConsumerOffsetCommandV5.class), //

	QUERY_MESSAGE_OFFSET_BY_TIME(105, 1, QueryMessageOffsetByTimeCommand.class), //

	MESSAGE_PULL_SPECIFIC(106, 1, PullSpecificMessageCommand.class), //

	FETCH_MANUAL_CONFIG_V6(107, 6, FetchManualConfigCommandV6.class), //

	ACK_MESSAGE_SEND(201, 1, SendMessageAckCommand.class), //
	ACK_MESSAGE_SEND_V5(201, 5, SendMessageAckCommandV5.class), //
	ACK_MESSAGE_SEND_V6(201, 6, SendMessageAckCommandV6.class), //

	ACK_MESSAGE_ACK_V5(202, 5, AckMessageAckCommandV5.class), //

	ACK_MESSAGE_PULL_V5(203, 5, PullMessageAckCommandV5.class), //

	ACK_QUERY_LATEST_CONSUMER_OFFSET_V5(204, 5, QueryLatestConsumerOffsetAckCommandV5.class), //

	RESULT_MESSAGE_SEND(301, 1, SendMessageResultCommand.class), //
	RESULT_MESSAGE_SEND_V6(301, 6, SendMessageResultCommandV6.class), //

	RESULT_MESSAGE_PULL(302, 1, PullMessageResultCommand.class), //
	RESULT_MESSAGE_PULL_V2(302, 2, PullMessageResultCommandV2.class), //
	RESULT_MESSAGE_PULL_V3(302, 3, PullMessageResultCommandV3.class), //
	RESULT_MESSAGE_PULL_V4(302, 4, PullMessageResultCommandV4.class), //
	RESULT_MESSAGE_PULL_V5(302, 5, PullMessageResultCommandV5.class), //

	RESULT_QUERY_OFFSET(303, 1, QueryOffsetResultCommand.class), //
	RESULT_QUERY_OFFSET_V3(303, 3, QueryOffsetResultCommandV3.class), //
	RESULT_QUERY_OFFSET_V5(303, 5, QueryOffsetResultCommandV5.class), //

	RESULT_ACK_MESSAGE_V3(304, 3, AckMessageResultCommandV3.class), //
	RESULT_ACK_MESSAGE_V5(304, 5, AckMessageResultCommandV5.class), //

	RESULT_FETCH_MANUAL_CONFIG_V6(305, 6, FetchManualConfigResultCommandV6.class), //
	;

	private static Map<Pair<Integer, Integer>, CommandType> m_types = new HashMap<>();

	static {
		for (CommandType type : CommandType.values()) {
			m_types.put(new Pair<Integer, Integer>(type.getType(), type.getVersion()), type);
		}
	}

	public static CommandType valueOf(int type, int version) {
		return m_types.get(new Pair<Integer, Integer>(type, version));
	}

	private int m_type;

	private int m_version;

	private Class<? extends Command> m_clazz;

	private CommandType(int type, int version, Class<? extends Command> clazz) {
		m_type = type;
		m_version = version;
		m_clazz = clazz;
	}

	public int getType() {
		return m_type;
	}

	public int getVersion() {
		return m_version;
	}

	public Class<? extends Command> getClazz() {
		return m_clazz;
	}

}
