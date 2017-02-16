package com.ctrip.hermes.core.transport.command.v5;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch.MessageMeta;
import com.ctrip.hermes.core.transport.ManualRelease;
import com.ctrip.hermes.core.transport.command.AbstractCommand;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@ManualRelease
public class PullMessageResultCommandV5 extends AbstractCommand {

	private static final long serialVersionUID = 1408373158921773649L;

	private List<TppConsumerMessageBatch> m_batches = new ArrayList<TppConsumerMessageBatch>();

	private Offset m_offset;

	public PullMessageResultCommandV5() {
		super(CommandType.RESULT_MESSAGE_PULL_V5, 5);
	}

	public List<TppConsumerMessageBatch> getBatches() {
		return m_batches;
	}

	public void setOffset(Offset offset) {
		m_offset = offset;
	}

	public Offset getOffset() {
		return m_offset;
	}

	public void addBatches(List<TppConsumerMessageBatch> batches) {
		if (batches != null) {
			m_batches.addAll(batches);
		}
	}

	@Override
	public void parse0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		List<TppConsumerMessageBatch> batches = new ArrayList<TppConsumerMessageBatch>();

		readBatchMetas(codec, batches);

		readBatchDatas(buf, codec, batches);

		m_batches = batches;

		m_offset = codec.readOffset();

	}

	@Override
	public void toBytes0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		writeBatchMetas(buf, codec, m_batches);
		writeBatchDatas(buf, codec, m_batches);
		codec.writeOffset(m_offset);
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
			int msgSize = codec.readInt();
			TppConsumerMessageBatch batch = new TppConsumerMessageBatch();
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

	private void writeBatchMetas(ByteBuf buf, HermesPrimitiveCodec codec, List<TppConsumerMessageBatch> batches) {
		codec.writeInt(batches.size());
		for (TppConsumerMessageBatch batch : batches) {
			codec.writeInt(batch.size());
			codec.writeString(batch.getTopic());
			codec.writeInt(batch.getPartition());
			codec.writeInt(batch.getPriority());
			codec.writeBoolean(batch.isResend());
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
