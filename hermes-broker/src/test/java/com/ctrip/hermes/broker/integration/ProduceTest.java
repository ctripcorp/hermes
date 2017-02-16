package com.ctrip.hermes.broker.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import com.ctrip.hermes.broker.dal.hermes.MessagePriority;
import com.ctrip.hermes.core.bo.SendMessageResult;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.transport.command.Command;
import com.ctrip.hermes.core.transport.command.SendMessageAckCommand;
import com.ctrip.hermes.core.transport.command.SendMessageCommand;
import com.ctrip.hermes.core.transport.command.v6.SendMessageResultCommandV6;

public class ProduceTest extends BaseBrokerTest {
	//
	// private String topic = "order_new";
	//
	// protected void doBefore() throws Exception {
	// }
	//
	// @Test
	// public void testSendOneNormalMessage() throws Exception {
	// ProducerMessage<String> pmsg = createProducerMessage(topic, uuid(), uuid(), 0, System.currentTimeMillis(), false);
	// testSendMessage(Arrays.asList(pmsg));
	// }
	//
	// @Test
	// public void testSendOneNormalMessageWithProps() throws Exception {
	// ProducerMessage<String> pmsg = createProducerMessage(topic, uuid(), uuid(), 0, System.currentTimeMillis(), false,
	// props(), props(), props());
	// testSendMessage(Arrays.asList(pmsg));
	// }
	//
	// @Test
	// public void testSendOnePriorityMessage() throws Exception {
	// ProducerMessage<String> pmsg = createProducerMessage(topic, uuid(), uuid(), 0, System.currentTimeMillis(), true);
	// testSendMessage(Arrays.asList(pmsg));
	// }
	//
	// @Test
	// public void testSendOnePriorityMessageWithProps() throws Exception {
	// ProducerMessage<String> pmsg = createProducerMessage(topic, uuid(), uuid(), 0, System.currentTimeMillis(), true,
	// props(), props(), props());
	// testSendMessage(Arrays.asList(pmsg));
	// }
	//
	// @Test
	// public void testSendMessageWithoutLease() throws Exception {
	// when(m_leaseContainer.acquireLease(anyString(), anyInt(), anyString())).thenReturn(null);
	//
	// ProducerMessage<String> pmsg = createProducerMessage(topic, uuid(), uuid(), 0, System.currentTimeMillis(), true,
	// props(), props(), props());
	// testSendMessage(Arrays.asList(pmsg), 0);
	// }
	//
	// @Test
	// public void testSendMessageWithLeaseRegain() throws Exception {
	// when(m_leaseContainer.acquireLease(anyString(), anyInt(), anyString())).thenReturn(null);
	// ProducerMessage<String> pmsg = createProducerMessage(topic, uuid(), uuid(), 0, System.currentTimeMillis(), true,
	// props(), props(), props());
	// testSendMessage(Arrays.asList(pmsg), 0);
	//
	// when(m_leaseContainer.acquireLease(anyString(), anyInt(), anyString())).thenReturn(new Lease(0, Long.MAX_VALUE));
	// pmsg = createProducerMessage(topic, uuid(), uuid(), 0, System.currentTimeMillis(), true, props(), props(),
	// props());
	// testSendMessage(Arrays.asList(pmsg));
	// }
	//
	// @Test
	// public void testSendMessageWithLeaseLost() throws Exception {
	// when(m_leaseContainer.acquireLease(anyString(), anyInt(), anyString())).thenReturn(new Lease(0, Long.MAX_VALUE));
	// ProducerMessage<String> pmsg = createProducerMessage(topic, uuid(), uuid(), 0, System.currentTimeMillis(), true,
	// props(), props(), props());
	// testSendMessage(Arrays.asList(pmsg));
	//
	// when(m_leaseContainer.acquireLease(anyString(), anyInt(), anyString())).thenReturn(null);
	// pmsg = createProducerMessage(topic, uuid(), uuid(), 0, System.currentTimeMillis(), true, props(), props(),
	// props());
	// testSendMessage(Arrays.asList(pmsg), 0);
	// }
	//
	// private void testSendMessage(List<ProducerMessage<String>> pmsgs, Integer... failIndexes) throws Exception {
	// Set<Integer> failMsgs = new HashSet<>(Arrays.asList(failIndexes));
	// boolean willReceiveSendMessageResultCommandV6 = failIndexes.length < pmsgs.size();
	//
	// final AtomicReference<SendMessageCommand> sendCmdRef = new AtomicReference<>();
	// final AtomicReference<SendMessageAckCommand> sendAckCmdRef = new AtomicReference<>();
	// final AtomicReference<SendMessageResultCommandV6> sendResultCmdRef = new AtomicReference<>();
	//
	// int resultCmdCount = 2;
	// if (!willReceiveSendMessageResultCommandV6) {
	// resultCmdCount = 1;
	// }
	// final CountDownLatch latch = new CountDownLatch(resultCmdCount);
	//
	// setCommandHandler(new CommandHandler() {
	// @Override
	// public void handle(Command arg) {
	// if (arg instanceof SendMessageAckCommand) {
	// sendAckCmdRef.set((SendMessageAckCommand) arg);
	// } else if (arg instanceof SendMessageResultCommandV6) {
	// sendResultCmdRef.set((SendMessageResultCommandV6) arg);
	// }
	// latch.countDown();
	// }
	// });
	//
	// sendCmdRef.set(sendMessage(topic, pmsgs));
	//
	// assertTrue(latch.await(3, TimeUnit.SECONDS));
	//
	// if (willReceiveSendMessageResultCommandV6) {
	// SendMessageCommand sendCmd = sendCmdRef.get();
	// SendMessageResultCommandV6 resultCmd = sendResultCmdRef.get();
	//
	// // verify ack
	// assertTrue(sendAckCmdRef.get().isSuccess());
	//
	// // verify correlation id
	// assertEquals(sendCmd.getHeader().getCorrelationId(), resultCmd.getHeader().getCorrelationId());
	//
	// // verify all futures are back
	// assertEquals(sendCmd.getFutures().size(), resultCmd.getResults().size());
	// for (Entry<Integer, SendMessageResult> entry : resultCmd.getResults().entrySet()) {
	// assertEquals(!failMsgs.contains(entry.getKey()), resultCmd.getResults().get(entry.getKey()));
	// }
	//
	// // verify messages are all saved to db
	// List<MessagePriority> rs = dumpMessagesInDB(topic);
	// assertEquals(pmsgs.size(), rs.size());
	//
	// Map<String, MessagePriority> refKey2Row = new HashMap<>();
	// for (MessagePriority row : rs) {
	// refKey2Row.put(row.getRefKey(), row);
	// }
	//
	// for (int i = 0; i < pmsgs.size(); i++) {
	// ProducerMessage<String> pmsg = pmsgs.get(i);
	// assertTrue(refKey2Row.containsKey(pmsg.getKey()));
	// MessagePriority row = refKey2Row.get(pmsg.getKey());
	// assertMessageEqual(pmsg, row);
	// }
	// } else {
	// assertFalse(sendAckCmdRef.get().isSuccess());
	// assertNull(sendResultCmdRef.get());
	// }
	// }

}
