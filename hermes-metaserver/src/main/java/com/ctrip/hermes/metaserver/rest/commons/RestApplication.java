package com.ctrip.hermes.metaserver.rest.commons;

import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class RestApplication extends ResourceConfig {
	public RestApplication() {
		register(DowngradeCheckFilter.class);
		register(CharsetResponseFilter.class);
		register(CORSResponseFilter.class);
		register(ObjectMapperProvider.class);
		register(MultiPartFeature.class);
		packages("com.ctrip.hermes.metaserver.rest.resource");
	}
}
