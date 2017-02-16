package com.ctrip.hermes.broker.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.dal.jdbc.configuration.AbstractJdbcResourceConfigurator;
import org.unidal.dal.jdbc.mapping.TableProvider;
import org.unidal.initialization.DefaultModuleManager;
import org.unidal.initialization.ModuleManager;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.broker.HermesBrokerModule;
import com.ctrip.hermes.broker.biz.logger.BrokerFileBizLogger;
import com.ctrip.hermes.broker.bootstrap.DefaultBrokerBootstrap;
import com.ctrip.hermes.broker.lease.BrokerLeaseContainer;
import com.ctrip.hermes.broker.lease.BrokerLeaseManager;
import com.ctrip.hermes.broker.longpolling.DefaultLongPollingService;
import com.ctrip.hermes.broker.longpolling.LongPollingService;
import com.ctrip.hermes.broker.meta.manual.BrokerManualConfigFetcher;
import com.ctrip.hermes.broker.queue.DefaultMessageQueueManager;
import com.ctrip.hermes.broker.queue.MessageQueueManager;
import com.ctrip.hermes.broker.queue.MessageQueuePartitionFactory;
import com.ctrip.hermes.broker.queue.storage.filter.DefaultFilter;
import com.ctrip.hermes.broker.queue.storage.kafka.KafkaMessageQueueStorage;
import com.ctrip.hermes.broker.queue.storage.mysql.MySQLMessageQueueStorage;
import com.ctrip.hermes.broker.queue.storage.mysql.ack.DefaultMySQLMessageAckFlusher;
import com.ctrip.hermes.broker.queue.storage.mysql.ack.MySQLMessageAckSelectorManager;
import com.ctrip.hermes.broker.queue.storage.mysql.dal.HermesTableProvider;
import com.ctrip.hermes.broker.queue.storage.mysql.dal.MessageDataSourceProvider;
import com.ctrip.hermes.broker.registry.DefaultBrokerRegistry;
import com.ctrip.hermes.broker.selector.DefaultPullMessageSelectorManager;
import com.ctrip.hermes.broker.selector.DefaultSendMessageSelectorManager;
import com.ctrip.hermes.broker.shutdown.ShutdownRequestMonitor;
import com.ctrip.hermes.broker.transport.NettyServer;
import com.ctrip.hermes.broker.transport.NettyServerConfig;
import com.ctrip.hermes.broker.transport.command.processor.AckMessageCommandProcessor;
import com.ctrip.hermes.broker.transport.command.processor.AckMessageCommandProcessorV3;
import com.ctrip.hermes.broker.transport.command.processor.AckMessageCommandProcessorV4;
import com.ctrip.hermes.broker.transport.command.processor.AckMessageCommandProcessorV5;
import com.ctrip.hermes.broker.transport.command.processor.FetchManualConfigCommandProcessorV6;
import com.ctrip.hermes.broker.transport.command.processor.PullMessageCommandProcessor;
import com.ctrip.hermes.broker.transport.command.processor.PullMessageCommandProcessorV3;
import com.ctrip.hermes.broker.transport.command.processor.PullMessageCommandProcessorV4;
import com.ctrip.hermes.broker.transport.command.processor.PullMessageCommandProcessorV5;
import com.ctrip.hermes.broker.transport.command.processor.PullSpecificMessageCommandProcessor;
import com.ctrip.hermes.broker.transport.command.processor.QueryLatestConsumerOffsetCommandProcessor;
import com.ctrip.hermes.broker.transport.command.processor.QueryLatestConsumerOffsetCommandProcessorV3;
import com.ctrip.hermes.broker.transport.command.processor.QueryLatestConsumerOffsetCommandProcessorV5;
import com.ctrip.hermes.broker.transport.command.processor.QueryMessageOffsetByTimeCommandProcessor;
import com.ctrip.hermes.broker.transport.command.processor.SendMessageCommandProcessor;
import com.ctrip.hermes.broker.transport.command.processor.SendMessageCommandProcessorV3;
import com.ctrip.hermes.broker.transport.command.processor.SendMessageCommandProcessorV5;
import com.ctrip.hermes.broker.transport.command.processor.SendMessageCommandProcessorV6;
import com.ctrip.hermes.broker.zk.ZKClient;
import com.ctrip.hermes.broker.zk.ZKConfig;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.meta.manual.ManualConfigService;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.env.config.broker.BrokerConfigProvider;

public class ComponentsConfigurator extends AbstractJdbcResourceConfigurator {

	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();

		all.add(A(DefaultBrokerBootstrap.class));
		all.add(A(NettyServer.class));
		all.add(A(NettyServerConfig.class));

		all.add(A(ShutdownRequestMonitor.class));

		all.add(A(BrokerManualConfigFetcher.class));

		all.add(A(BrokerFileBizLogger.class));

		all.add(C(CommandProcessor.class, CommandType.MESSAGE_SEND.toString(), SendMessageCommandProcessor.class)//
		      .req(MessageQueueManager.class)//
		      .req(BrokerLeaseContainer.class)//
		      .req(BrokerConfigProvider.class)//
		      .req(BrokerFileBizLogger.class)//
		      .req(MetaService.class)//
		      .req(SystemClockService.class)//
		);
		all.add(C(CommandProcessor.class, CommandType.MESSAGE_SEND_V3.toString(), SendMessageCommandProcessorV3.class)//
		      .req(MessageQueueManager.class)//
		      .req(BrokerLeaseContainer.class)//
		      .req(BrokerConfigProvider.class)//
		      .req(BrokerFileBizLogger.class)//
		      .req(MetaService.class)//
		      .req(SystemClockService.class)//
		);
		all.add(C(CommandProcessor.class, CommandType.MESSAGE_SEND_V5.toString(), SendMessageCommandProcessorV5.class)//
		      .req(MessageQueueManager.class)//
		      .req(BrokerLeaseContainer.class)//
		      .req(BrokerConfigProvider.class)//
		      .req(BrokerFileBizLogger.class)//
		      .req(MetaService.class)//
		      .req(SystemClockService.class)//
		);
		all.add(C(CommandProcessor.class, CommandType.MESSAGE_SEND_V6.toString(), SendMessageCommandProcessorV6.class)//
		      .req(MessageQueueManager.class)//
		      .req(BrokerLeaseContainer.class)//
		      .req(BrokerConfigProvider.class)//
		      .req(BrokerFileBizLogger.class)//
		      .req(MetaService.class)//
		      .req(SystemClockService.class)//
		);
		all.add(C(CommandProcessor.class, CommandType.MESSAGE_PULL.toString(), PullMessageCommandProcessor.class)//
		      .req(LongPollingService.class)//
		      .req(BrokerLeaseContainer.class)//
		      .req(BrokerConfigProvider.class)//
		      .req(MetaService.class)//
		);
		all.add(C(CommandProcessor.class, CommandType.MESSAGE_PULL_V3.toString(), PullMessageCommandProcessorV3.class)//
		      .req(LongPollingService.class)//
		      .req(BrokerLeaseContainer.class)//
		      .req(BrokerConfigProvider.class)//
		      .req(MetaService.class)//
		);
		all.add(C(CommandProcessor.class, CommandType.MESSAGE_PULL_V4.toString(), PullMessageCommandProcessorV4.class)//
		      .req(LongPollingService.class)//
		      .req(BrokerLeaseContainer.class)//
		      .req(BrokerConfigProvider.class)//
		      .req(MetaService.class)//
		);
		all.add(C(CommandProcessor.class, CommandType.MESSAGE_PULL_V5.toString(), PullMessageCommandProcessorV5.class)//
		      .req(LongPollingService.class)//
		      .req(BrokerLeaseContainer.class)//
		      .req(BrokerConfigProvider.class)//
		      .req(MetaService.class)//
		);
		all.add(C(CommandProcessor.class, CommandType.MESSAGE_PULL_SPECIFIC.toString(),
		      PullSpecificMessageCommandProcessor.class)//
		      .req(BrokerLeaseContainer.class)//
		      .req(MessageQueueManager.class)//
		      .req(BrokerConfigProvider.class)//
		);
		all.add(C(CommandProcessor.class, CommandType.MESSAGE_ACK.toString(), AckMessageCommandProcessor.class)//
		      .req(MessageQueueManager.class)//
		      .req(BrokerFileBizLogger.class) //
		      .req(MetaService.class) //
		);
		all.add(C(CommandProcessor.class, CommandType.MESSAGE_ACK_V3.toString(), AckMessageCommandProcessorV3.class)//
		      .req(MessageQueueManager.class)//
		      .req(BrokerFileBizLogger.class) //
		      .req(MetaService.class) //
		      .req(SystemClockService.class) //
		);
		all.add(C(CommandProcessor.class, CommandType.MESSAGE_ACK_V4.toString(), AckMessageCommandProcessorV4.class)//
		      .req(MessageQueueManager.class)//
		      .req(BrokerFileBizLogger.class) //
		      .req(MetaService.class) //
		      .req(SystemClockService.class) //
		);
		all.add(C(CommandProcessor.class, CommandType.MESSAGE_ACK_V5.toString(), AckMessageCommandProcessorV5.class)//
		      .req(MessageQueueManager.class)//
		      .req(BrokerFileBizLogger.class) //
		      .req(BrokerLeaseContainer.class)//
		      .req(MetaService.class) //
		      .req(BrokerConfigProvider.class)//
		      .req(SystemClockService.class) //
		);
		all.add(C(CommandProcessor.class, CommandType.QUERY_LATEST_CONSUMER_OFFSET.toString(),
		      QueryLatestConsumerOffsetCommandProcessor.class)//
		      .req(BrokerLeaseContainer.class)//
		      .req(BrokerConfigProvider.class)//
		      .req(MetaService.class)//
		      .req(MessageQueueManager.class)//
		);
		all.add(C(CommandProcessor.class, CommandType.QUERY_LATEST_CONSUMER_OFFSET_V3.toString(),
		      QueryLatestConsumerOffsetCommandProcessorV3.class)//
		      .req(BrokerLeaseContainer.class)//
		      .req(BrokerConfigProvider.class)//
		      .req(MetaService.class)//
		      .req(MessageQueueManager.class)//
		);
		all.add(C(CommandProcessor.class, CommandType.QUERY_LATEST_CONSUMER_OFFSET_V5.toString(),
		      QueryLatestConsumerOffsetCommandProcessorV5.class)//
		      .req(BrokerLeaseContainer.class)//
		      .req(BrokerConfigProvider.class)//
		      .req(MetaService.class)//
		      .req(MessageQueueManager.class)//
		);
		all.add(C(CommandProcessor.class, CommandType.QUERY_MESSAGE_OFFSET_BY_TIME.toString(),
		      QueryMessageOffsetByTimeCommandProcessor.class)//
		      .req(BrokerLeaseContainer.class)//
		      .req(BrokerConfigProvider.class)//
		      .req(MetaService.class)//
		      .req(MessageQueueManager.class)//
		);
		all.add(C(CommandProcessor.class, CommandType.QUERY_MESSAGE_OFFSET_BY_TIME.toString(),
		      QueryMessageOffsetByTimeCommandProcessor.class)//
		      .req(BrokerLeaseContainer.class)//
		      .req(BrokerConfigProvider.class)//
		      .req(MetaService.class)//
		      .req(MessageQueueManager.class)//
		);
		all.add(C(CommandProcessor.class, CommandType.FETCH_MANUAL_CONFIG_V6.toString(),
		      FetchManualConfigCommandProcessorV6.class)//
		      .req(ManualConfigService.class)//
		);

		all.add(A(DefaultLongPollingService.class));
		all.add(A(BrokerLeaseManager.class));
		all.add(A(BrokerLeaseContainer.class));

		all.add(A(DefaultFilter.class));

		all.add(A(MessageQueuePartitionFactory.class));
		all.add(A(DefaultMessageQueueManager.class));
		all.add(A(MySQLMessageQueueStorage.class));
		all.add(A(KafkaMessageQueueStorage.class));

		all.add(C(TableProvider.class, "message-priority", HermesTableProvider.class) //
		      .req(MetaService.class));
		all.add(C(TableProvider.class, "resend-group-id", HermesTableProvider.class) //
		      .req(MetaService.class));
		all.add(C(TableProvider.class, "offset-message", HermesTableProvider.class) //
		      .req(MetaService.class));
		all.add(C(TableProvider.class, "offset-resend", HermesTableProvider.class) //
		      .req(MetaService.class));
		all.add(C(TableProvider.class, "dead-letter", HermesTableProvider.class) //
		      .req(MetaService.class));

		all.add(A(MySQLMessageAckSelectorManager.class));
		all.add(A(DefaultMySQLMessageAckFlusher.class));

		all.add(A(MessageDataSourceProvider.class));

		all.add(A(DefaultBrokerRegistry.class));
		all.add(A(ZKClient.class));
		all.add(A(ZKConfig.class));

		all.add(A(DefaultPullMessageSelectorManager.class));
		all.add(A(DefaultSendMessageSelectorManager.class));

		all.addAll(new HermesDatabaseConfigurator().defineComponents());

		all.add(C(ModuleManager.class, DefaultModuleManager.class) //
		      .config(E("topLevelModules").value(HermesBrokerModule.ID)));

		return all;
	}

	public static void main(String[] args) {
		generatePlexusComponentsXmlFile(new ComponentsConfigurator());
	}
}
