package com.ctrip.hermes.metaserver.rest.commons;

import java.io.IOException;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.env.ManualConfigProvider;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class DowngradeCheckFilter implements ContainerRequestFilter {

	@Override
	public void filter(ContainerRequestContext requestContext) throws IOException {
		if (PlexusComponentLocator.lookup(ManualConfigProvider.class).isManualConfigureModeOn()) {
			requestContext.abortWith(Response.status(Status.FORBIDDEN).build());
		}
	}

}
