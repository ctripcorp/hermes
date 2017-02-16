package com.ctrip.hermes.core.pipeline;

import java.util.List;

import com.ctrip.hermes.core.pipeline.spi.Valve;

public interface ValveRegistry {
	public void register(Valve valve, String name, int order);

	public List<Valve> getValveList();

}
