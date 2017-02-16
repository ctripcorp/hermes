package com.ctrip.hermes.producer.pipeline;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.pipeline.AbstractValveRegistry;
import com.ctrip.hermes.core.pipeline.ValveRegistry;
import com.ctrip.hermes.producer.build.BuildConstants;

@Named(type = ValveRegistry.class, value = BuildConstants.PRODUCER)
public class ProducerValveRegistry extends AbstractValveRegistry implements Initializable {

	@Override
	public void initialize() throws InitializationException {
		doRegister(EnrichMessageValve.ID, 0);
		doRegister(TracingMessageValve.ID, 1);
	}

}
