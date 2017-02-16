package com.ctrip.hermes.metaserver.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.dal.jdbc.configuration.AbstractJdbcResourceConfigurator;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.metaserver.assign.LeastAdjustmentAssianBalancer;
import com.ctrip.hermes.metaserver.broker.BrokerAssignmentHolder;
import com.ctrip.hermes.metaserver.broker.BrokerLeaseHolder;
import com.ctrip.hermes.metaserver.broker.DefaultBrokerLeaseAllocator;
import com.ctrip.hermes.metaserver.broker.LeastAdjustmentBrokerPartitionAssigningStrategy;
import com.ctrip.hermes.metaserver.broker.endpoint.MetaEndpointClient;
import com.ctrip.hermes.metaserver.cluster.ClusterStateHolder;
import com.ctrip.hermes.metaserver.commons.EndpointMaker;
import com.ctrip.hermes.metaserver.config.MetaServerConfig;
import com.ctrip.hermes.metaserver.consumer.ActiveConsumerListHolder;
import com.ctrip.hermes.metaserver.consumer.ConsumerAssignmentHolder;
import com.ctrip.hermes.metaserver.consumer.ConsumerLeaseHolder;
import com.ctrip.hermes.metaserver.consumer.DefaultConsumerLeaseAllocator;
import com.ctrip.hermes.metaserver.consumer.DefaultConsumerLeaseAllocatorLocator;
import com.ctrip.hermes.metaserver.consumer.LeastAdjustmentConsumerPartitionAssigningStrategy;
import com.ctrip.hermes.metaserver.event.DefaultEventBus;
import com.ctrip.hermes.metaserver.event.DefaultEventHandlerRegistry;
import com.ctrip.hermes.metaserver.event.Guard;
import com.ctrip.hermes.metaserver.event.impl.BaseMetaChangedEventHandler;
import com.ctrip.hermes.metaserver.event.impl.BrokerLeaseChangedEventHandler;
import com.ctrip.hermes.metaserver.event.impl.BrokerListChangedEventHandler;
import com.ctrip.hermes.metaserver.event.impl.DefaultLeaderMetaFetcher;
import com.ctrip.hermes.metaserver.event.impl.FollowerInitEventHandler;
import com.ctrip.hermes.metaserver.event.impl.LeaderInitEventHandler;
import com.ctrip.hermes.metaserver.event.impl.MetaServerListChangedEventHandler;
import com.ctrip.hermes.metaserver.event.impl.ObserverInitEventHandler;
import com.ctrip.hermes.metaserver.meta.LeastAdjustmentMetaServerAssigningStrategy;
import com.ctrip.hermes.metaserver.meta.MetaHolder;
import com.ctrip.hermes.metaserver.meta.MetaServerAssignmentHolder;
import com.ctrip.hermes.metaserver.monitor.DefaultQueryOffsetResultMonitor;
import com.ctrip.hermes.metaserver.monitor.QueryOffsetResultMonitor;
import com.ctrip.hermes.metaserver.processor.QueryOffsetResultCommandProcessor;

public class ComponentsConfigurator extends AbstractJdbcResourceConfigurator {

	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();

		all.add(A(MetaHolder.class));
		all.add(A(MetaServerConfig.class));

		// consumer lease
		all.add(A(DefaultConsumerLeaseAllocator.class));
		all.add(A(ActiveConsumerListHolder.class));
		all.add(A(ConsumerLeaseHolder.class));
		all.add(A(DefaultConsumerLeaseAllocatorLocator.class));

		// broker lease
		all.add(A(DefaultBrokerLeaseAllocator.class));
		all.add(A(BrokerLeaseHolder.class));

		// cluster
		all.add(A(ClusterStateHolder.class));

		// assignment
		all.add(A(ConsumerAssignmentHolder.class));
		all.add(A(LeastAdjustmentConsumerPartitionAssigningStrategy.class));
		all.add(A(BrokerAssignmentHolder.class));
		all.add(A(LeastAdjustmentBrokerPartitionAssigningStrategy.class));
		all.add(A(MetaServerAssignmentHolder.class));
		all.add(A(LeastAdjustmentMetaServerAssigningStrategy.class));
		all.add(A(LeastAdjustmentAssianBalancer.class));

		// event handler
		all.add(A(DefaultEventHandlerRegistry.class));
		all.add(A(EndpointMaker.class));

		all.add(A(DefaultEventBus.class));
		all.add(A(Guard.class));

		// leader
		all.add(A(LeaderInitEventHandler.class));
		all.add(A(BaseMetaChangedEventHandler.class));
		all.add(A(BrokerListChangedEventHandler.class));
		all.add(A(MetaServerListChangedEventHandler.class));
		all.add(A(BrokerLeaseChangedEventHandler.class));

		// follower
		all.add(A(FollowerInitEventHandler.class));
		all.add(A(DefaultLeaderMetaFetcher.class));

		// observer
		all.add(A(ObserverInitEventHandler.class));

		// endpoint client
		all.add(A(MetaEndpointClient.class));

		// message offsets handler
		all.add(C(CommandProcessor.class, CommandType.RESULT_QUERY_OFFSET.toString(),
		      QueryOffsetResultCommandProcessor.class)//
		      .req(QueryOffsetResultMonitor.class));
		all.add(A(DefaultQueryOffsetResultMonitor.class));

		return all;
	}

	public static void main(String[] args) {
		generatePlexusComponentsXmlFile(new ComponentsConfigurator());
	}
}
