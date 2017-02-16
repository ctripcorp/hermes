package com.ctrip.hermes.producer.pipeline;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Future;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.pipeline.PipelineSink;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Storage;

@Named(type = ProducerPipelineSinkManager.class)
public class DefaultProducerPipelineSinkManager extends ContainerHolder implements Initializable,
      ProducerPipelineSinkManager {

	@Inject
	private MetaService m_meta;

	private Map<String, PipelineSink<Future<SendResult>>> m_sinks = new HashMap<String, PipelineSink<Future<SendResult>>>();

	@Override
	public PipelineSink<Future<SendResult>> getSink(String topic) {
		Storage storage = m_meta.findStorageByTopic(topic);
		String endpointType = Storage.KAFKA.equals(storage.getType()) ? Endpoint.KAFKA : //
		      Storage.MYSQL.equals(storage.getType()) ? Endpoint.BROKER : null;

		if (m_sinks.containsKey(endpointType)) {
			return m_sinks.get(endpointType);
		} else {
			throw new IllegalArgumentException(String.format("Unknown message sink for topic %s", topic));
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void initialize() throws InitializationException {
		Map<String, PipelineSink> sinks = lookupMap(PipelineSink.class);

		for (Entry<String, PipelineSink> entry : sinks.entrySet()) {
			m_sinks.put(entry.getKey(), entry.getValue());
		}
	}
}
