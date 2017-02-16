package com.ctrip.hermes.core.cmessaging;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.unidal.helper.Reflects;
import org.unidal.lookup.ComponentTestCase;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.cmessaging.entity.Cmessaging;
import com.ctrip.hermes.cmessaging.transform.DefaultSaxParser;

public class DefaultCMessagingConfigServiceTest extends ComponentTestCase {

	@Test
	public void xxxxxxx() throws Exception {
		Cmessaging cmsg = DefaultSaxParser.parse(this.getClass().getResourceAsStream("consume_both.xml"));
		System.out.println(JSON.toJSONString(cmsg));

	}

	@Test
	public void testAllClose() throws Exception {
		DefaultCMessagingConfigService s = createServiceWithXml("all_close.xml");
		assertEquals(CMessagingConfigService.CONSUME_FROM_CMESSAGING, s.getConsumerType("ex1", "id1", "1.1.1.1"));
		assertNull(s.getGroupId("ex1", "id1"));
		assertNull(s.getTopic("ex1"));
		assertFalse(s.isHermesProducerEnabled("ex1", "id1", "1.1.1.1"));
	}

	@Test
	public void testConsume() throws Exception {
		DefaultCMessagingConfigService s = createServiceWithXml("consume_both.xml");

		assertEquals(CMessagingConfigService.CONSUME_FROM_CMESSAGING, s.getConsumerType("ex1", "cid0", "1.1.1.1"));
		assertEquals(CMessagingConfigService.CONSUME_FROM_CMESSAGING, s.getConsumerType("ex1", "cid1", "1.1.1.3"));
		assertEquals(CMessagingConfigService.CONSUME_FROM_CMESSAGING, s.getConsumerType("ex1", "unknown", "1.1.1.1"));
		assertEquals(CMessagingConfigService.CONSUME_FROM_CMESSAGING, s.getConsumerType("ex2", "unknown", "1.1.1.1"));
		assertEquals(CMessagingConfigService.CONSUME_FROM_CMESSAGING, s.getConsumerType("ex10", "unknown", "1.1.1.1"));

		assertEquals(CMessagingConfigService.CONSUME_FROM_BOTH, s.getConsumerType("ex1", "cid1", "1.1.1.1"));
		assertEquals(CMessagingConfigService.CONSUME_FROM_BOTH, s.getConsumerType("ex1", "cid1", "1.1.1.2"));

		assertEquals(CMessagingConfigService.CONSUME_FROM_BOTH, s.getConsumerType("ex1", "cid2", "1.1.1.1"));
		assertEquals(CMessagingConfigService.CONSUME_FROM_BOTH, s.getConsumerType("ex1", "cid2", "1.1.1.3"));

		assertEquals(CMessagingConfigService.CONSUME_FROM_BOTH, s.getConsumerType("ex3", "unknown", "1.1.1.3"));

		assertEquals(CMessagingConfigService.CONSUME_FROM_HERMES, s.getConsumerType("ex1", "cid3", "1.1.1.1"));
		assertEquals(CMessagingConfigService.CONSUME_FROM_HERMES, s.getConsumerType("ex1", "cid3", "1.1.1.3"));
		assertEquals(CMessagingConfigService.CONSUME_FROM_HERMES, s.getConsumerType("ex4", "cid1", "1.1.1.3"));

	}

	@Test
	public void testProduce() throws Exception {
		DefaultCMessagingConfigService s = createServiceWithXml("consume_both.xml");
		assertFalse(s.isHermesProducerEnabled("ex1", "pid0", "2.2.2.1"));
		assertFalse(s.isHermesProducerEnabled("ex1", "pid0", "2.2.2.3"));
		assertFalse(s.isHermesProducerEnabled("ex1", "pid3", "2.2.2.1"));
		assertFalse(s.isHermesProducerEnabled("ex2", "pid0", "2.2.2.1"));
		assertFalse(s.isHermesProducerEnabled("ex10", "pid0", "2.2.2.1"));

		assertTrue(s.isHermesProducerEnabled("ex1", "pid1", "2.2.2.1"));
		assertTrue(s.isHermesProducerEnabled("ex1", "pid1", "2.2.2.2"));
		assertFalse(s.isHermesProducerEnabled("ex1", "pid1", "2.2.2.3"));

		assertTrue(s.isHermesProducerEnabled("ex1", "pid2", "2.2.2.1"));
		assertTrue(s.isHermesProducerEnabled("ex1", "pid2", "2.2.2.3"));
		assertTrue(s.isHermesProducerEnabled("ex3", "pid10", "2.2.2.3"));
	}

	@Test
	public void testGetGroupId() throws Exception {
		DefaultCMessagingConfigService s = createServiceWithXml("consume_both.xml");
		assertEquals("hermes-cid0", s.getGroupId("ex1", "cid0"));
		assertEquals("hermes-cid1", s.getGroupId("ex1", "cid1"));
		assertNull(s.getGroupId("ex1", "cid10"));
	}

	@Test
	public void testGetTopic() throws Exception {
		DefaultCMessagingConfigService s = createServiceWithXml("consume_both.xml");
		assertEquals("hermes-ex1", s.getTopic("ex1"));
		assertEquals("hermes-ex2", s.getTopic("ex2"));
		assertNull(s.getTopic("ex10"));
	}

	@SuppressWarnings("unchecked")
	private DefaultCMessagingConfigService createServiceWithXml(String fileName) throws Exception {
		DefaultCMessagingConfigService cs = new DefaultCMessagingConfigService();
		Field f = Reflects.forField().getDeclaredField(DefaultCMessagingConfigService.class, "m_cmsgConfigCache");
		f.setAccessible(true);
		AtomicReference<Cmessaging> cache = (AtomicReference<Cmessaging>) f.get(cs);

		cache.set(DefaultSaxParser.parse(this.getClass().getResourceAsStream(fileName)));

		return cs;
	}

}
