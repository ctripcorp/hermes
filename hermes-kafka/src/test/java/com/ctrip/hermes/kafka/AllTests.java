package com.ctrip.hermes.kafka;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.ctrip.hermes.kafka.avro.KafkaAvroTest;
import com.ctrip.hermes.kafka.producer.ProducerTest;

@RunWith(Suite.class)
@SuiteClasses({//
OneBoxTest.class, ProducerTest.class, KafkaAvroTest.class })
public class AllTests {

}
