package com.ctrip.hermes.broker.transport.command.processor;

import java.util.Arrays;
import java.util.List;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.core.meta.manual.ManualConfig;
import com.ctrip.hermes.core.meta.manual.ManualConfigService;
import com.ctrip.hermes.core.transport.ChannelUtils;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;
import com.ctrip.hermes.core.transport.command.v6.FetchManualConfigCommandV6;
import com.ctrip.hermes.core.transport.command.v6.FetchManualConfigResultCommandV6;

/**
 * 
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class FetchManualConfigCommandProcessorV6 implements CommandProcessor {

	@Inject
	private ManualConfigService m_manualConfigService;

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.FETCH_MANUAL_CONFIG_V6);
	}

	@Override
	public void process(final CommandProcessorContext ctx) {
		FetchManualConfigCommandV6 reqCmd = (FetchManualConfigCommandV6) ctx.getCommand();
		long version = reqCmd.getVersion();
		ManualConfig manualConfig = m_manualConfigService.getConfig();

		FetchManualConfigResultCommandV6 respCmd = new FetchManualConfigResultCommandV6();
		respCmd.correlate(reqCmd);

		if (manualConfig != null
		      && (version == FetchManualConfigCommandV6.UNSET_VERSION || manualConfig.getVersion() != version)) {
			respCmd.setData(manualConfig.toBytes());
		} else {
			respCmd.setData(null);
		}

		ChannelUtils.writeAndFlush(ctx.getChannel(), respCmd);
	}

}
