package com.ctrip.hermes.kafka.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.lookup.configuration.AbstractResourceConfigurator;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.core.pipeline.PipelineSink;
import com.ctrip.hermes.kafka.engine.KafkaConsumerBootstrap;
import com.ctrip.hermes.kafka.producer.KafkaMessageSender;
import com.ctrip.hermes.kafka.service.KafkaService;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.producer.config.ProducerConfig;
import com.ctrip.hermes.producer.pipeline.DefaultProducerPipelineSink;
import com.ctrip.hermes.producer.sender.MessageSender;

public class ComponentsConfigurator extends AbstractResourceConfigurator {

	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();

		all.add(C(PipelineSink.class, Endpoint.KAFKA, DefaultProducerPipelineSink.class) //
		      .req(MessageSender.class, Endpoint.KAFKA).req(ProducerConfig.class));
		all.add(A(KafkaMessageSender.class));

		all.add(A(KafkaConsumerBootstrap.class));
		
		all.add(A(KafkaService.class));
		
		return all;
	}

	public static void main(String[] args) {
		generatePlexusComponentsXmlFile(new ComponentsConfigurator());
	}
}
