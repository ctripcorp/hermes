package com.ctrip.hermes.core.transport.command;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.util.ArrayList;
import java.util.List;

import com.ctrip.hermes.core.message.TppConsumerMessageBatch;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch.MessageMeta;
import com.ctrip.hermes.core.transport.ManualRelease;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@ManualRelease
public class PullMessageResultCommand extends AbstractCommand {
	private static final long serialVersionUID = 7125716603747372895L;

	private List<TppConsumerMessageBatch> m_batches = new ArrayList<TppConsumerMessageBatch>();

	private transient Channel m_channel;

	public PullMessageResultCommand() {
		super(CommandType.RESULT_MESSAGE_PULL, 1);
	}

	public List<TppConsumerMessageBatch> getBatches() {
		return m_batches;
	}

	public void addBatches(List<TppConsumerMessageBatch> batches) {
		if (batches != null) {
			m_batches.addAll(batches);
		}
	}

	public Channel getChannel() {
		return m_channel;
	}

	public void setChannel(Channel channel) {
		m_channel = channel;
	}

	@Override
	public void parse0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		List<TppConsumerMessageBatch> batches = new ArrayList<TppConsumerMessageBatch>();

		readBatchMetas(codec, batches);

		readBatchDatas(buf, codec, batches);

		m_batches = batches;
	}

	@Override
	public void toBytes0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		writeBatchMetas(codec, m_batches);
		writeBatchDatas(buf, codec, m_batches);
	}

	private void writeBatchDatas(ByteBuf buf, HermesPrimitiveCodec codec, List<TppConsumerMessageBatch> batches) {
		for (TppConsumerMessageBatch batch : batches) {
			// placeholder for len
			int start = buf.writerIndex();
			codec.writeInt(-1);
			int indexBeforeData = buf.writerIndex();
			batch.getTransferCallback().transfer(buf);
			int indexAfterData = buf.writerIndex();

			buf.writerIndex(start);
			codec.writeInt(indexAfterData - indexBeforeData);
			buf.writerIndex(indexAfterData);

		}
	}

	private void readBatchDatas(ByteBuf buf, HermesPrimitiveCodec codec, List<TppConsumerMessageBatch> batches) {
		for (TppConsumerMessageBatch batch : batches) {
			int len = codec.readInt();
			batch.setData(buf.readSlice(len));
		}

	}

	private void readBatchMetas(HermesPrimitiveCodec codec, List<TppConsumerMessageBatch> batches) {
		int batchSize = codec.readInt();
		for (int i = 0; i < batchSize; i++) {
			TppConsumerMessageBatch batch = new TppConsumerMessageBatch();
			int msgSize = codec.readInt();
			batch.setTopic(codec.readString());
			batch.setPartition(codec.readInt());
			batch.setPriority(codec.readInt());
			batch.setResend(codec.readBoolean());

			for (int j = 0; j < msgSize; j++) {
				batch.addMessageMeta(new MessageMeta(codec.readLong(), codec.readInt(), codec.readLong(), codec.readInt(),
				      codec.readBoolean()));
			}
			batches.add(batch);
		}
	}

	private void writeBatchMetas(HermesPrimitiveCodec codec, List<TppConsumerMessageBatch> batches) {
		codec.writeInt(batches.size());
		for (TppConsumerMessageBatch batch : batches) {
			codec.writeInt(batch.size());
			codec.writeString(batch.getTopic());
			codec.writeInt(batch.getPartition());
			codec.writeInt(batch.getPriority());
			codec.writeBoolean(batch.isResend());
			if (batch.size() > 0) {
				for (MessageMeta msgMeta : batch.getMessageMetas()) {
					codec.writeLong(msgMeta.getId());
					codec.writeInt(msgMeta.getRemainingRetries());
					codec.writeLong(msgMeta.getOriginId());
					codec.writeInt(msgMeta.getPriority());
					codec.writeBoolean(msgMeta.isResend());
				}
			}
		}
	}
}
