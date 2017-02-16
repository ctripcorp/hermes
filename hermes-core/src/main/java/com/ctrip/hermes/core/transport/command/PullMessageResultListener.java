package com.ctrip.hermes.core.transport.command;

import com.ctrip.hermes.core.transport.command.v5.PullMessageResultCommandV5;

public interface PullMessageResultListener extends Command {
	public void onResultReceived(PullMessageResultCommandV5 ack);
}
