package com.ctrip.hermes.producer;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.ctrip.hermes.producer.integration.ProducerIntegrationTest;

@RunWith(Suite.class)
@SuiteClasses({ //
ProducerIntegrationTest.class, //
})
public class AllTests {

}
