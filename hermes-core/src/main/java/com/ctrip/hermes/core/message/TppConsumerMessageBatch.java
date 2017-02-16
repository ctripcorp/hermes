package com.ctrip.hermes.core.message;

import java.util.ArrayList;
import java.util.List;

import com.ctrip.hermes.core.transport.TransferCallback;

import io.netty.buffer.ByteBuf;

/**
 * mapping to one <topic, partition, priority, isResend>
 * 
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class TppConsumerMessageBatch {
	private String m_topic;

	private int m_partition;

	private boolean m_resend;

	private int m_priority;

	private List<MessageMeta> m_messageMetas = new ArrayList<MessageMeta>();

	private TransferCallback m_transferCallback;

	private ByteBuf data;

	public TppConsumerMessageBatch() {
	}

	public boolean isResend() {
		return m_resend;
	}

	public void setResend(boolean resend) {
		m_resend = resend;
	}

	public int getPriority() {
		return m_priority;
	}

	public void setPriority(int priority) {
		m_priority = priority;
	}

	public int getPartition() {
		return m_partition;
	}

	public void setPartition(int partition) {
		m_partition = partition;
	}

	public ByteBuf getData() {
		return data;
	}

	public void setData(ByteBuf data) {
		this.data = data;
	}

	public String getTopic() {
		return m_topic;
	}

	public void setTopic(String topic) {
		m_topic = topic;
	}

	public List<MessageMeta> getMessageMetas() {
		return m_messageMetas;
	}

	public void addMessageMeta(MessageMeta msgMeta) {
		m_messageMetas.add(msgMeta);
	}

	public void addMessageMetas(List<MessageMeta> msgMetas) {
		m_messageMetas.addAll(msgMetas);
	}

	public TransferCallback getTransferCallback() {
		return m_transferCallback;
	}

	public void setTransferCallback(TransferCallback transferCallback) {
		m_transferCallback = transferCallback;
	}

	public int size() {
		return m_messageMetas.size();
	}

	public static class DummyMessageMeta extends MessageMeta {
		public DummyMessageMeta(long id, int remainingRetries, long originId, int priority, boolean isResend) {
			super(id, remainingRetries, originId, priority, isResend);
		}
	}

	public static class MessageMeta {
		private long m_id;

		private int m_remainingRetries;

		private long m_originId;

		private int m_priority;

		private boolean m_resend;

		public MessageMeta(long id, int remainingRetries, long originId, int priority, boolean isResend) {
			m_id = id;
			m_remainingRetries = remainingRetries;
			m_originId = originId;
			m_priority = priority;
			m_resend = isResend;
		}

		public boolean isResend() {
			return m_resend;
		}

		public int getRemainingRetries() {
			return m_remainingRetries;
		}

		public long getId() {
			return m_id;
		}

		public long getOriginId() {
			return m_originId;
		}

		public int getPriority() {
			return m_priority;
		}

		public void setRemainingRetries(int remainingRetries) {
			m_remainingRetries = remainingRetries;
		}

	}

	public boolean isPriority() {
		return m_priority == 0;
	}
}
