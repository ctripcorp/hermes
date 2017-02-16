package com.ctrip.hermes.metaserver;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.ctrip.hermes.metaserver.assign.AssignBalancerTest;
import com.ctrip.hermes.metaserver.broker.DefaultBrokerPartitionAssigningStrategyTest;
import com.ctrip.hermes.metaserver.commons.EndpointMakerTest;
import com.ctrip.hermes.metaserver.consumer.ActiveConsumerListHolderTest;
import com.ctrip.hermes.metaserver.consumer.ActiveConsumerListTest;
import com.ctrip.hermes.metaserver.consumer.ConsumerAssignmentHolderTest;
import com.ctrip.hermes.metaserver.consumer.LeastAdjustmentOrderedConsumeConsumerPartitionAssigningStrategyTest;
import com.ctrip.hermes.metaserver.event.FollowerEventEngineTest;
import com.ctrip.hermes.metaserver.event.LeaderEventEngineTest;

@RunWith(Suite.class)
@SuiteClasses({ //
// MetaServerBrokerAssignmentTest.class, //
// MetaServerAssignmentTest.class,//
// MetaServerBaseMetaChangeTest.class, //
// MetaServerBrokerLeaseTest.class,//
// MetaServerBrokerLeaseChangedTest.class,//
// MetaServerConsumerLeaseTest.class,//
// // MetaServerConsumerLeaseChangeTest.class,//
// MetaServerLeadershipTest.class,//
LeastAdjustmentOrderedConsumeConsumerPartitionAssigningStrategyTest.class,//
      AssignBalancerTest.class,//
      DefaultBrokerPartitionAssigningStrategyTest.class, //
      // DefaultBrokerLeaseAllocatorTest.class, //
      // BrokerLeaseHolderTest.class, //
      ActiveConsumerListTest.class, //
      ConsumerAssignmentHolderTest.class, //
      // OrderedConsumeConsumerLeaseAllocatorTest.class, //
      // ConsumerLeaseHolderTest.class, //
      ActiveConsumerListHolderTest.class, //
      EndpointMakerTest.class, //
      FollowerEventEngineTest.class, //
      LeaderEventEngineTest.class, //
// add test classes here

})
public class AllTests {
	//
}
