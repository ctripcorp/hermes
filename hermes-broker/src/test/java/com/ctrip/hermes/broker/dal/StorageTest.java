package com.ctrip.hermes.broker.dal;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.Date;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.broker.dal.hermes.MessagePriority;
import com.ctrip.hermes.broker.dal.hermes.MessagePriorityDao;
import com.ctrip.hermes.broker.dal.hermes.MessagePriorityEntity;
import com.ctrip.hermes.broker.dal.hermes.OffsetMessage;
import com.ctrip.hermes.broker.dal.hermes.OffsetMessageDao;
import com.ctrip.hermes.broker.dal.hermes.OffsetMessageEntity;
import com.ctrip.hermes.broker.dal.hermes.ResendGroupId;
import com.ctrip.hermes.broker.dal.hermes.ResendGroupIdDao;
import com.ctrip.hermes.broker.dal.hermes.ResendGroupIdEntity;
import com.ctrip.hermes.core.bo.Tpp;

public class StorageTest extends ComponentTestCase {
	//
	// private MessagePriorityDao msgDao;
	//
	// private OffsetMessageDao msgOffsetDao;
	//
	// private ResendGroupIdDao resendDao;
	//
	// // private OffsetResendDao resendOffsetDao;
	//
	// private String topic = "order_new";
	//
	// int partition = 0;
	//
	// private int groupId = 100;
	//
	// @Before
	// public void before() {
	// msgDao = lookup(MessagePriorityDao.class);
	// msgOffsetDao = lookup(OffsetMessageDao.class);
	// resendDao = lookup(ResendGroupIdDao.class);
	// // resendOffsetDao = lookup(OffsetResendDao.class);
	// }
	//
	// @Test
	// public void full() throws Exception {
	// Tpp tpp = new Tpp(topic, partition, true);
	// List<MessagePriority> wMsgs = MessageUtil.makeMessages(tpp, 5);
	// for (MessagePriority wMsg : wMsgs) {
	// appendMessage(wMsg);
	// }
	// List<MessagePriority> rMsgs = readMessage();
	//
	// assertEquals(wMsgs.size(), rMsgs.size());
	//
	// appendResend(rMsgs.get(1));
	//
	// updateMessageOffset(rMsgs.get(rMsgs.size() - 1));
	//
	// List<ResendGroupId> resends = readResendMessage();
	//
	// assertEquals(1, resends.size());
	// assertMessageResendEquals(rMsgs.get(1), resends.get(0));
	//
	// updateResendOffset(resends.get(0));
	//
	// }
	//
	// private void updateResendOffset(ResendGroupId resendGroupId) {
	// }
	//
	// private void assertMessageResendEquals(MessagePriority msg, ResendGroupId resend) {
	// assertArrayEquals(msg.getPayload(), resend.getPayload());
	// // TODO assert other fields
	// }
	//
	// private List<ResendGroupId> readResendMessage() throws DalException {
	// return resendDao.find(topic, partition, groupId, 10, 1L, 3, ResendGroupIdEntity.READSET_FULL);
	// }
	//
	// private void updateMessageOffset(MessagePriority msg) throws DalException {
	// String topic = msg.getTopic();
	// int partition = msg.getPartition();
	// int priority = msg.getPriority();
	//
	// List<OffsetMessage> offsets = msgOffsetDao.find(topic, partition, priority, groupId,
	// OffsetMessageEntity.READSET_FULL);
	//
	// if (offsets.isEmpty()) {
	// OffsetMessage initOffset = new OffsetMessage();
	// initOffset.setCreationDate(new Date());
	// initOffset.setGroupId(groupId);
	// initOffset.setOffset(msg.getId());
	// initOffset.setPartition(partition);
	// initOffset.setPriority(priority);
	// initOffset.setTopic(topic);
	//
	// msgOffsetDao.insert(initOffset);
	// } else {
	// OffsetMessage offset = offsets.get(0);
	// offset.setTopic(topic);
	// offset.setPartition(partition);
	//
	// offset.setOffset(msg.getId());
	//
	// msgOffsetDao.updateByPK(offset, OffsetMessageEntity.UPDATESET_OFFSET);
	// }
	// }
	//
	// private void appendResend(MessagePriority msg) throws DalException {
	// ResendGroupId r = new ResendGroupId();
	//
	// r.setAttributes(msg.getAttributes());
	// r.setCreationDate(new Date());
	// r.setGroupId(groupId);
	// r.setPartition(msg.getPartition());
	// r.setPayload(msg.getPayload());
	// r.setProducerId(msg.getProducerId());
	// r.setProducerIp(msg.getProducerIp());
	// r.setRefKey(msg.getRefKey());
	// r.setRemainingRetries(5);
	// r.setScheduleDate(new Date());
	// r.setTopic(msg.getTopic());
	//
	// resendDao.insert(r);
	// }
	//
	// private List<MessagePriority> readMessage() throws DalException {
	// List<MessagePriority> msgs = msgDao.findIdAfter(topic, 0, 0, 0, 10, MessagePriorityEntity.READSET_FULL);
	//
	// for (MessagePriority msg : msgs) {
	// msg.setTopic(topic);
	// msg.setPartition(0);
	// msg.setPriority(0);
	// }
	//
	// return msgs;
	// }
	//
	// private void appendMessage(MessagePriority msg) throws DalException {
	// msgDao.insert(msg);
	// }
	//
	// @Test
	// public void testFind() throws Exception {
	// List<MessagePriority> result = msgDao.findIdAfter(topic, 0, 0, 0, 10, MessagePriorityEntity.READSET_FULL);
	// for (MessagePriority r : result) {
	// System.out.println(r);
	// }
	// }
	//
	// @Test
	// public void testInsert() throws Exception {
	// Tpp tpp = new Tpp(topic, partition, true);
	// MessagePriority m = MessageUtil.makeMessage(tpp);
	//
	// msgDao.insert(m);
	// }

}
