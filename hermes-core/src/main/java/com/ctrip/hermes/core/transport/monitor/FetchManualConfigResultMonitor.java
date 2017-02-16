package com.ctrip.hermes.core.transport.monitor;

import java.util.concurrent.Future;

import com.ctrip.hermes.core.meta.manual.ManualConfig;
import com.ctrip.hermes.core.transport.command.v6.FetchManualConfigCommandV6;
import com.ctrip.hermes.core.transport.command.v6.FetchManualConfigResultCommandV6;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface FetchManualConfigResultMonitor {

	Future<ManualConfig> monitor(FetchManualConfigCommandV6 cmd);

	void cancel(FetchManualConfigCommandV6 cmd);

	void resultReceived(FetchManualConfigResultCommandV6 result);

}
