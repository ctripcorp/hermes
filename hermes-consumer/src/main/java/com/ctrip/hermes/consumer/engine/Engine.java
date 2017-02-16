package com.ctrip.hermes.consumer.engine;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;

public abstract class Engine {

	public abstract SubscribeHandle start(Subscriber subscriber);

	public static Engine getInstance() {
		return PlexusComponentLocator.lookup(Engine.class);
	}
}
