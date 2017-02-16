package com.ctrip.hermes.producer.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.lookup.configuration.AbstractResourceConfigurator;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.core.kafka.KafkaIdcStrategy;
import com.ctrip.hermes.core.message.partition.PartitioningStrategy;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.pipeline.PipelineSink;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.endpoint.EndpointClient;
import com.ctrip.hermes.core.transport.endpoint.EndpointManager;
import com.ctrip.hermes.env.ClientEnvironment;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.producer.DefaultProducer;
import com.ctrip.hermes.producer.HermesProducerModule;
import com.ctrip.hermes.producer.config.ProducerConfig;
import com.ctrip.hermes.producer.monitor.DefaultSendMessageAcceptanceMonitor;
import com.ctrip.hermes.producer.monitor.DefaultSendMessageResultMonitor;
import com.ctrip.hermes.producer.monitor.SendMessageAcceptanceMonitor;
import com.ctrip.hermes.producer.monitor.SendMessageResultMonitor;
import com.ctrip.hermes.producer.pipeline.DefaultProducerPipelineSink;
import com.ctrip.hermes.producer.pipeline.DefaultProducerPipelineSinkManager;
import com.ctrip.hermes.producer.pipeline.EnrichMessageValve;
import com.ctrip.hermes.producer.pipeline.ProducerPipeline;
import com.ctrip.hermes.producer.pipeline.ProducerValveRegistry;
import com.ctrip.hermes.producer.pipeline.TracingMessageValve;
import com.ctrip.hermes.producer.sender.BrokerMessageSender;
import com.ctrip.hermes.producer.sender.DefaultMessageSenderSelectorManager;
import com.ctrip.hermes.producer.sender.MessageSender;
import com.ctrip.hermes.producer.sender.MessageSenderSelectorManager;
import com.ctrip.hermes.producer.transport.command.processor.SendMessageAckCommandProcessor;
import com.ctrip.hermes.producer.transport.command.processor.SendMessageResultCommandProcessor;

public class ComponentsConfigurator extends AbstractResourceConfigurator {

	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();

		all.add(A(HermesProducerModule.class));

		all.add(A(DefaultProducer.class));
		all.add(A(ProducerPipeline.class));
		all.add(A(ProducerValveRegistry.class));

		// valves
		all.add(A(TracingMessageValve.class));
		all.add(A(EnrichMessageValve.class));

		// sinks
		all.add(A(DefaultProducerPipelineSinkManager.class));
		all.add(C(PipelineSink.class, Endpoint.BROKER, DefaultProducerPipelineSink.class) //
		      .req(MessageSender.class, Endpoint.BROKER)//
		      .req(ProducerConfig.class)//
		);

		// message sender
		all.add(C(MessageSender.class, Endpoint.BROKER, BrokerMessageSender.class)//
		      .req(EndpointManager.class)//
		      .req(PartitioningStrategy.class)//
		      .req(MetaService.class)//
		      .req(SendMessageAcceptanceMonitor.class)//
		      .req(SendMessageResultMonitor.class)//
		      .req(ProducerConfig.class)//
		      .req(EndpointClient.class)//
		      .req(MessageSenderSelectorManager.class)//
		      .req(ClientEnvironment.class)//
		      .req(KafkaIdcStrategy.class)//
		);

		// command processors
		all.add(C(CommandProcessor.class, CommandType.ACK_MESSAGE_SEND_V6.toString(),
		      SendMessageAckCommandProcessor.class)//
		      .req(SendMessageAcceptanceMonitor.class));
		all.add(C(CommandProcessor.class, CommandType.RESULT_MESSAGE_SEND_V6.toString(),
		      SendMessageResultCommandProcessor.class)//
		      .req(SendMessageResultMonitor.class));

		// monitors
		all.add(A(DefaultSendMessageResultMonitor.class));
		all.add(A(DefaultSendMessageAcceptanceMonitor.class));

		all.add(A(DefaultMessageSenderSelectorManager.class));

		all.add(A(ProducerConfig.class));

		return all;
	}

	public static void main(String[] args) {
		generatePlexusComponentsXmlFile(new ComponentsConfigurator());
	}
}
