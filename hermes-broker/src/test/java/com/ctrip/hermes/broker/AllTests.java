package com.ctrip.hermes.broker;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.ctrip.hermes.broker.ack.internal.DefaultAckHolderTest;
import com.ctrip.hermes.broker.queue.storage.mysql.cache.DefaultMessageCacheTest;
import com.ctrip.hermes.broker.queue.storage.mysql.cache.DefaultPageCacheTest;

@RunWith(Suite.class)
@SuiteClasses({//
DefaultAckHolderTest.class,//
      DefaultPageCacheTest.class,//
      DefaultMessageCacheTest.class,//
      // add test classes here

})
public class AllTests {

}
