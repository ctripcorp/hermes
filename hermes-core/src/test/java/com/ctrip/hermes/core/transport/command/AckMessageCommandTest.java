package com.ctrip.hermes.core.transport.command;

import static org.junit.Assert.assertEquals;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import org.junit.Test;
import org.unidal.tuple.Triple;

import com.ctrip.hermes.core.HermesCoreBaseTest;
import com.ctrip.hermes.core.bo.AckContext;
import com.ctrip.hermes.core.bo.Tpp;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class AckMessageCommandTest extends HermesCoreBaseTest {

	@Test
	public void test() throws Exception {
		AckMessageCommand cmd = new AckMessageCommand();
		cmd.addAckMsg(new Tpp("topic1", 1, true), "group1", true, 1, 1, 0, 1);
		cmd.addAckMsg(new Tpp("topic1", 1, true), "group1", true, 2, 1, 0, 1);
		cmd.addAckMsg(new Tpp("topic1", 1, false), "group1", false, 3, 0, 10, 15);
		cmd.addAckMsg(new Tpp("topic1", 2, false), "group2", false, 4, 0, 100, 115);
		cmd.addAckMsg(new Tpp("topic2", 2, false), "group2", false, 5, 0, 12, 13);

		cmd.addNackMsg(new Tpp("topic1", 1, true), "group1", true, 10, 1, 0, 1);
		cmd.addNackMsg(new Tpp("topic1", 1, false), "group1", false, 11, 0, 10, 15);
		cmd.addNackMsg(new Tpp("topic2", 2, false), "group2", false, 12, 0, 12, 13);

		ByteBuf buf = Unpooled.buffer();
		cmd.toBytes(buf);

		AckMessageCommand decodedCmd = new AckMessageCommand();
		Header header = new Header();
		header.parse(buf);
		decodedCmd.parse(buf, header);

		ConcurrentMap<Triple<Tpp, String, Boolean>, List<AckContext>> ackMsgs = decodedCmd.getAckMsgs();
		assertEquals(4, ackMsgs.size());

		assertEquals(Arrays.asList(new AckContext(1, 1, 0, 1), new AckContext(2, 1, 0, 1)),
		      ackMsgs.get(new Triple<Tpp, String, Boolean>(new Tpp("topic1", 1, true), "group1", true)));

		assertEquals(Arrays.asList(new AckContext(3, 0, 10, 15)),
		      ackMsgs.get(new Triple<Tpp, String, Boolean>(new Tpp("topic1", 1, false), "group1", false)));

		assertEquals(Arrays.asList(new AckContext(4, 0, 100, 115)),
		      ackMsgs.get(new Triple<Tpp, String, Boolean>(new Tpp("topic1", 2, false), "group2", false)));
		assertEquals(Arrays.asList(new AckContext(5, 0, 12, 13)),
		      ackMsgs.get(new Triple<Tpp, String, Boolean>(new Tpp("topic2", 2, false), "group2", false)));

		ConcurrentMap<Triple<Tpp, String, Boolean>, List<AckContext>> nackMsgs = decodedCmd.getNackMsgs();
		assertEquals(3, nackMsgs.size());
		assertEquals(Arrays.asList(new AckContext(10, 1, 0, 1)),
		      nackMsgs.get(new Triple<Tpp, String, Boolean>(new Tpp("topic1", 1, true), "group1", true)));
		assertEquals(Arrays.asList(new AckContext(11, 0, 10, 15)),
		      nackMsgs.get(new Triple<Tpp, String, Boolean>(new Tpp("topic1", 1, false), "group1", false)));
		assertEquals(Arrays.asList(new AckContext(12, 0, 12, 13)),
		      nackMsgs.get(new Triple<Tpp, String, Boolean>(new Tpp("topic2", 2, false), "group2", false)));
	}

}
