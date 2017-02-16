package com.ctrip.hermes.broker.dal;

import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.broker.dal.hermes.*;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;
import com.google.common.base.Charsets;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class MessageDaoTest extends ComponentTestCase {
	//
	// @BeforeClass
	// public static void bc() {
	// System.setProperty("devMode", "true");
	// }
	//
	// @Test
	// public void testRaw() throws Exception {
	// MysqlDataSource ds = new MysqlDataSource();
	// ds.setUrl("jdbc:mysql://127.0.0.1/100_0?rewriteBatchedStatements=true");
	// ds.setUser("root");
	//
	// String sql = "INSERT INTO `message_0` "
	// + "(`producer_ip`, `producer_id`, `ref_key`, `attributes`, `creation_date`, `payload`)"
	// + " VALUES (?,?,?,?,?,?)";
	// Connection conn = ds.getConnection();
	// conn.setAutoCommit(false);
	// PreparedStatement stmt = conn.prepareStatement(sql);
	//
	// for (int i = 0; i < 5; i++) {
	// stmt.setString(1, "xx");
	// stmt.setLong(2, 1);
	// stmt.setString(3, "xx");
	// stmt.setString(4, "xx");
	// stmt.setDate(5, new Date(System.currentTimeMillis()));
	// stmt.setBlob(6, new ByteArrayInputStream("hello".getBytes()));
	//
	// stmt.addBatch();
	// }
	// stmt.executeBatch();
	// conn.commit();
	// stmt.clearBatch();
	//
	// for (int i = 0; i < 6000; i++) {
	// stmt.setString(1, "xx");
	// stmt.setLong(2, 1);
	// stmt.setString(3, "xx");
	// stmt.setString(4, "xx");
	// stmt.setDate(5, new Date(System.currentTimeMillis()));
	// ByteArrayInputStream in = new ByteArrayInputStream("xx".getBytes());
	// stmt.setBlob(6, in, in.available());
	//
	// stmt.addBatch();
	// }
	//
	// long start = System.currentTimeMillis();
	// int[] xx = stmt.executeBatch();
	// System.out.println(xx.length);
	// conn.commit();
	// System.out.println(System.currentTimeMillis() - start);
	//
	// }
	//
	// @Test
	// public void testDao() throws Exception {
	// MessagePriorityDao dao = lookup(MessagePriorityDao.class);
	//
	// MessagePriority[] msgs = new MessagePriority[6000];
	// Tpp tpp = new Tpp("order_new", 0, true);
	// for (int i = 0; i < msgs.length; i++) {
	// msgs[i] = MessageUtil.makeMessage(tpp);
	// }
	//
	// dao.insert(MessageUtil.makeMessage(tpp));
	//
	// long start = System.currentTimeMillis();
	// dao.insert(msgs);
	// System.out.println(System.currentTimeMillis() - start);
	// }
	//
	// @Test
	// public void testDaoWithStrangeString() throws Exception {
	// final String stangeString = "{\"1\":{\"str\":\"429bb071-7d14-4da7-9ef1-a6f5b17911b5\"}," +
	// "\"2\":{\"str\":\"ExchangeTest\"},\"3\":{\"i32\":8},\"4\":{\"str\":\"uft-8\"},\"5\":{\"str\":\"cmessage-adapter 1.0\"},\"6\":{\"i32\":3},\"7\":{\"i32\":1},\"8\":{\"i32\":0},\"9\":{\"str\":\"order_new\"},\"10\":{\"str\":\"\"},\"11\":{\"str\":\"1\"},\"12\":{\"str\":\"DST56615\"},\"13\":{\"str\":\"555555\"},\"14\":{\"str\":\"169.254.142.159\"},\"15\":{\"str\":\"java.lang.String\"},\"16\":{\"i64\":1429168996889},\"17\":{\"map\":[\"str\",\"str\",0,{}]}}";
	//
	// System.out.println(new String(stangeString.getBytes(Charsets.UTF_8), Charsets.UTF_8).equals(stangeString));
	//
	// Map<String, String> map = new HashMap<>();
	// map.put("strange", stangeString);
	//
	// ByteBuf buf = Unpooled.buffer();
	// HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
	// codec.writeStringStringMap(map);
	//
	// byte[] bytes = new byte[buf.readableBytes()];
	// buf.readBytes(bytes);
	//
	// MessagePriority mp = new MessagePriority();
	// mp.setTopic("order_new");
	// mp.setPartition(0);
	// mp.setPriority(0);
	// mp.setAttributes(bytes);
	//
	// ByteBuf buf2 = (Unpooled.wrappedBuffer(mp.getAttributes()));
	//
	// HermesPrimitiveCodec codec2 = new HermesPrimitiveCodec(buf2);
	//
	// System.out.println(codec2.readStringStringMap());
	//
	// MessagePriorityDao dao = lookup(MessagePriorityDao.class);
	//
	// dao.insert(mp);
	//
	// // 需要清空表，因为取第一个
	// List<MessagePriority> m = dao.findIdAfter("order_new", 0, 0, 0L, 1, MessagePriorityEntity.READSET_FULL);
	//
	// ByteBuf buf3 = (Unpooled.wrappedBuffer(m.get(0).getAttributes()));
	//
	// HermesPrimitiveCodec codec3 = new HermesPrimitiveCodec(buf3);
	//
	// System.out.println(codec3.readStringStringMap());
	// }
	//
	// @Test
	// public void test() throws DalException {
	// ResendGroupIdDao resendDao = lookup(ResendGroupIdDao.class);
	// ResendGroupId proto = new ResendGroupId();
	// proto.setTopic("order_new");
	// proto.setPartition(0);
	// proto.setPriority(0);
	// proto.setGroupId(100);
	// proto.setScheduleDate(new Date(System.currentTimeMillis()));
	// proto.setRemainingRetries(5);
	// proto.setMessageIds(new Long[] { 1L, 2L });
	//
	// resendDao.copyFromMessageTable(proto);
	// }

}
