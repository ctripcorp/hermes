package com.ctrip.hermes.core.transport.command;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

import com.ctrip.hermes.core.message.PartialDecodedMessage;
import com.ctrip.hermes.core.message.codec.MessageCodec;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;

public class MessageBatchWithRawData {
	private String m_topic;

	private List<Integer> m_msgSeqs;

	private ByteBuf m_rawData;

	private List<PartialDecodedMessage> m_msgs;

	private long m_selectorOffset;

	private String m_targetIdc;
	
	public MessageBatchWithRawData(String topic, List<Integer> msgSeqs, ByteBuf rawData, String targetIdc) {
		m_topic = topic;
		m_msgSeqs = msgSeqs;
		m_rawData = rawData;
		m_targetIdc = targetIdc == null ? PlexusComponentLocator.lookup(MetaService.class).getPrimaryIdc().getId() : targetIdc;
	}

	public long getSelectorOffset() {
		return m_selectorOffset;
	}

	public void setSelectorOffset(long selectorOffset) {
		m_selectorOffset = selectorOffset;
	}

	public String getTopic() {
		return m_topic;
	}

	public List<Integer> getMsgSeqs() {
		return m_msgSeqs;
	}

	public ByteBuf getRawData() {
		return m_rawData.duplicate();
	}
	
	public String getTargetIdc() {
		return m_targetIdc;
	}

	public List<PartialDecodedMessage> getMessages() {

		if (m_msgs == null) {
			synchronized (this) {
				if (m_msgs == null) {
					m_msgs = new ArrayList<PartialDecodedMessage>();

					ByteBuf tmpBuf = m_rawData.duplicate();
					MessageCodec messageCodec = PlexusComponentLocator.lookup(MessageCodec.class);

					while (tmpBuf.readableBytes() > 0) {
						m_msgs.add(messageCodec.decodePartial(tmpBuf));
					}

				}
			}
		}

		return m_msgs;
	}
}
