package com.ctrip.hermes.consumer.integration.assist;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.message.PartialDecodedMessage;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch.MessageMeta;
import com.ctrip.hermes.core.message.codec.MessageCodec;
import com.ctrip.hermes.core.message.payload.JsonPayloadCodec;
import com.ctrip.hermes.core.transport.TransferCallback;
import com.ctrip.hermes.core.transport.command.Header;
import com.ctrip.hermes.core.transport.command.v5.PullMessageResultCommandV5;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Codec;

public class PullMessageResultCreator {
	private static void writeProperties(List<Pair<String, String>> properties, ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		if (properties != null) {
			Map<String, String> map = new HashMap<String, String>();
			for (Pair<String, String> prop : properties) {
				map.put(prop.getKey(), prop.getValue());
			}

			codec.writeStringStringMap(map);
		} else {
			codec.writeNull();
		}
	}

	private static PartialDecodedMessage createPartialDecodedMessage(String topic, String key, Object body,
	      List<Pair<String, String>> durableProperties, int remainingRetries, long bornTime) {
		PartialDecodedMessage partialMsg = new PartialDecodedMessage();

		partialMsg.setRemainingRetries(remainingRetries);
		ByteBuf buf = Unpooled.buffer();
		writeProperties(durableProperties, buf);
		partialMsg.setDurableProperties(buf);
		partialMsg.setBody(Unpooled.wrappedBuffer(new JsonPayloadCodec().encode(topic, body)));
		partialMsg.setBornTime(bornTime);
		partialMsg.setKey(key);
		partialMsg.setBodyCodecType(Codec.JSON);

		return partialMsg;
	}

	private static void addBatch(List<TppConsumerMessageBatch> batches, String topic, int partition,
	      final List<Pair<MessageMeta, PartialDecodedMessage>> msgs, boolean isResend, int priority) {
		TppConsumerMessageBatch batch = new TppConsumerMessageBatch();
		List<MessageMeta> metas = new ArrayList<MessageMeta>();
		for (Pair<MessageMeta, PartialDecodedMessage> msg : msgs) {
			metas.add(msg.getKey());
		}
		batch.addMessageMetas(metas);

		batch.setTopic(topic);
		batch.setPartition(partition);
		batch.setResend(isResend);
		batch.setPriority(priority);

		batch.setTransferCallback(new TransferCallback() {
			@Override
			public void transfer(ByteBuf out) {
				MessageCodec msgCodec = PlexusComponentLocator.lookup(MessageCodec.class);
				if (msgCodec != null) {
					for (Pair<MessageMeta, PartialDecodedMessage> msg : msgs) {
						msgCodec.encodePartial(msg.getValue(), out);
					}
				} else {
					System.out.println("********** [ERROR] Can not find MessageCodec!");
				}
			}
		});
		batches.add(batch);
	}

	private static Pair<MessageMeta, PartialDecodedMessage> createMsgAndMetas(long id, String topic, String key,
	      Object body, List<Pair<String, String>> durableProperties, long bornTime, int remainingRetries, long originId,
	      int priority, boolean isResend) {
		Pair<MessageMeta, PartialDecodedMessage> pair = new Pair<MessageMeta, PartialDecodedMessage>();
		pair.setKey(new MessageMeta(id, remainingRetries, originId, priority, isResend));
		pair.setValue(createPartialDecodedMessage(topic, key, body, durableProperties, remainingRetries, bornTime));
		return pair;
	}

	public static <T> PullMessageResultCommandV5 createPullMessageResultCommand(String topic,
	      List<Pair<String, String>> attributes, int remainingRetries, int priority, boolean isResend, String keyPrefix,
	      List<List<T>> batchedMessages) {
		long bornTime = System.currentTimeMillis();

		PullMessageResultCommandV5 cmd = new PullMessageResultCommandV5();
		List<TppConsumerMessageBatch> batches = new ArrayList<TppConsumerMessageBatch>();
		long id = 0;
		for (List<?> rawMsgs : batchedMessages) {
			List<Pair<MessageMeta, PartialDecodedMessage>> msgs = new ArrayList<>();
			for (Object msg : rawMsgs) {
				msgs.add(createMsgAndMetas(id++, topic, keyPrefix + id, msg, attributes, bornTime, remainingRetries, 0,
				      priority, isResend));
			}
			addBatch(batches, topic, 1, msgs, isResend, priority);
		}

		cmd.addBatches(batches);

		ByteBuf buf = Unpooled.buffer();
		cmd.toBytes(buf);

		PullMessageResultCommandV5 decodedCmd = new PullMessageResultCommandV5();
		Header header = new Header();
		header.parse(buf);
		decodedCmd.parse(buf, header);

		return decodedCmd;
	}
}
