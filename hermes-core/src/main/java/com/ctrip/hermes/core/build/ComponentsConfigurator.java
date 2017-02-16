package com.ctrip.hermes.core.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.lookup.configuration.AbstractResourceConfigurator;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.core.cmessaging.DefaultCMessagingConfigService;
import com.ctrip.hermes.core.config.CoreConfig;
import com.ctrip.hermes.core.kafka.KafkaIdcStrategy;
import com.ctrip.hermes.core.log.CatBizLogger;
import com.ctrip.hermes.core.log.CatFileBizLogger;
import com.ctrip.hermes.core.log.FileBizLogger;
import com.ctrip.hermes.core.message.codec.DefaultMessageCodec;
import com.ctrip.hermes.core.message.partition.HashPartitioningStrategy;
import com.ctrip.hermes.core.message.payload.AvroPayloadCodec;
import com.ctrip.hermes.core.message.payload.CMessagingPayloadCodec;
import com.ctrip.hermes.core.message.payload.DeflaterPayloadCodec;
import com.ctrip.hermes.core.message.payload.GZipPayloadCodec;
import com.ctrip.hermes.core.message.payload.JsonPayloadCodec;
import com.ctrip.hermes.core.message.payload.PayloadCodec;
import com.ctrip.hermes.core.message.payload.assist.HermesKafkaAvroDeserializer;
import com.ctrip.hermes.core.message.payload.assist.HermesKafkaAvroSerializer;
import com.ctrip.hermes.core.message.payload.assist.SchemaRegisterRestClient;
import com.ctrip.hermes.core.meta.internal.DefaultMetaManager;
import com.ctrip.hermes.core.meta.internal.DefaultMetaService;
import com.ctrip.hermes.core.meta.manual.DefaultManualConfigFetcher;
import com.ctrip.hermes.core.meta.manual.DefaultManualConfigService;
import com.ctrip.hermes.core.meta.remote.DefaultMetaServerLocator;
import com.ctrip.hermes.core.meta.remote.RemoteMetaLoader;
import com.ctrip.hermes.core.meta.remote.RemoteMetaProxy;
import com.ctrip.hermes.core.service.DefaultSystemClockService;
import com.ctrip.hermes.core.service.RunningStatusStatisticsService;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorManager;
import com.ctrip.hermes.core.transport.command.processor.DefaultCommandProcessorRegistry;
import com.ctrip.hermes.core.transport.endpoint.DefaultEndpointClient;
import com.ctrip.hermes.core.transport.endpoint.DefaultEndpointManager;
import com.ctrip.hermes.core.transport.monitor.DefaultFetchManualConfigResultMonitor;
import com.ctrip.hermes.core.transport.monitor.FetchManualConfigResultMonitor;
import com.ctrip.hermes.core.transport.processor.FetchManualConfigResultCommandProcessorV6;
import com.ctrip.hermes.meta.entity.Codec;

public class ComponentsConfigurator extends AbstractResourceConfigurator {

	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();

		// partition algo
		all.add(A(HashPartitioningStrategy.class));

		// meta
		all.add(A(RemoteMetaLoader.class));
		all.add(A(DefaultMetaManager.class));
		all.add(A(DefaultMetaService.class));
		all.add(A(RemoteMetaProxy.class));
		all.add(A(DefaultMetaServerLocator.class));
		all.add(A(DefaultManualConfigService.class));
		all.add(A(DefaultManualConfigFetcher.class));

		// endpoint manager
		all.add(A(DefaultEndpointManager.class));

		// endpoint Client
		all.add(A(DefaultEndpointClient.class));

		// command processor
		all.add(A(CommandProcessorManager.class));
		all.add(A(DefaultCommandProcessorRegistry.class));

		// codec
		all.add(A(DefaultMessageCodec.class));
		all.add(A(JsonPayloadCodec.class));
		all.add(A(SchemaRegisterRestClient.class).is(PER_LOOKUP));
		all.add(A(HermesKafkaAvroSerializer.class));
		all.add(A(HermesKafkaAvroDeserializer.class));
		all.add(C(HermesKafkaAvroDeserializer.class, "GENERIC", HermesKafkaAvroDeserializer.class) //
		      .req(SchemaRegisterRestClient.class));
		all.add(A(AvroPayloadCodec.class));
		all.add(A(GZipPayloadCodec.class));
		all.add(A(CMessagingPayloadCodec.class));

		for (int i = 1; i <= 9; i++) {
			all.add(C(PayloadCodec.class, Codec.DEFLATER + "-" + i, DeflaterPayloadCodec.class)//
			      .config(E("level").value(Integer.toString(i))));
		}

		all.add(A(CoreConfig.class));
		all.add(A(DefaultSystemClockService.class));

		all.add(A(FileBizLogger.class));
		all.add(A(CatBizLogger.class));
		all.add(A(CatFileBizLogger.class));

		all.add(A(RunningStatusStatisticsService.class));

		// cmessaging config service
		all.add(A(DefaultCMessagingConfigService.class));

		all.add(A(DefaultFetchManualConfigResultMonitor.class));
		all.add(C(CommandProcessor.class, CommandType.RESULT_FETCH_MANUAL_CONFIG_V6,
		      FetchManualConfigResultCommandProcessorV6.class)//
		      .req(FetchManualConfigResultMonitor.class)//
		);

		all.add(A(KafkaIdcStrategy.class));

		return all;
	}

	public static void main(String[] args) {
		generatePlexusComponentsXmlFile(new ComponentsConfigurator());
	}
}
