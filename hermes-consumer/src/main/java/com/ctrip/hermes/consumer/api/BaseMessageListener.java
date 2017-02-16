package com.ctrip.hermes.consumer.api;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.core.constants.CatConstants;
import com.ctrip.hermes.core.message.BaseConsumerMessage;
import com.ctrip.hermes.core.message.BaseConsumerMessageAware;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.message.ConsumerMessage.MessageStatus;
import com.ctrip.hermes.core.message.PropertiesHolder;
import com.ctrip.hermes.core.message.PropertiesHolderAware;
import com.dianping.cat.Cat;
import com.dianping.cat.configuration.NetworkInterfaceManager;
import com.dianping.cat.message.Event;
import com.dianping.cat.message.Transaction;
import com.dianping.cat.message.internal.DefaultTransaction;
import com.dianping.cat.message.spi.MessageTree;

public abstract class BaseMessageListener<T> implements MessageListener<T> {
	private static final Logger log = LoggerFactory.getLogger(BaseMessageListener.class);

	private String m_groupId;

	public BaseMessageListener() {
	}

	public void setGroupId(String groupId) {
		this.m_groupId = groupId;
	}

	@Override
	public void onMessage(List<ConsumerMessage<T>> msgs) {
		if (msgs != null && !msgs.isEmpty()) {
			String topic = msgs.get(0).getTopic();

			for (ConsumerMessage<T> msg : msgs) {
				Transaction t = Cat.newTransaction(CatConstants.TYPE_MESSAGE_CONSUMED, topic + ":" + m_groupId);
				MessageTree tree = Cat.getManager().getThreadLocalMessageTree();

				if (msg instanceof PropertiesHolderAware) {
					PropertiesHolder holder = ((PropertiesHolderAware) msg).getPropertiesHolder();
					String rootMsgId = holder.getDurableSysProperty(CatConstants.ROOT_MESSAGE_ID);
					String parentMsgId = holder.getDurableSysProperty(CatConstants.CURRENT_MESSAGE_ID);

					tree.setRootMessageId(rootMsgId);
					tree.setParentMessageId(parentMsgId);
				}

				try {
					t.addData("topic", topic);
					t.addData("key", msg.getRefKey());
					t.addData("groupId", m_groupId);

					setOnMessageStartTime(msg);
					onMessage(msg);
					setOnMessageEndTime(msg);
					// by design, if nacked, no effect
					msg.ack();

					String ip = NetworkInterfaceManager.INSTANCE.getLocalHostAddress();
					Cat.logEvent("Consumer:" + ip, msg.getTopic() + ":" + m_groupId, Event.SUCCESS, "key=" + msg.getRefKey());
					Cat.logEvent("Message:" + topic, "Consumed:" + ip, Event.SUCCESS, "key=" + msg.getRefKey());
					Cat.logMetricForCount(msg.getTopic());
					t.setStatus(MessageStatus.SUCCESS.equals(msg.getStatus()) ? Transaction.SUCCESS : "FAILED-WILL-RETRY");
				} catch (Exception e) {
					Cat.logError(e);
					t.setStatus(e);
					log.error("Exception occurred while calling onMessage.", e);
					msg.nack();
				} finally {
					t.complete();
				}
			}

		}
	}

	private void setOnMessageEndTime(ConsumerMessage<T> msg) {
		if (msg instanceof BaseConsumerMessageAware) {
			BaseConsumerMessage<?> baseMsg = ((BaseConsumerMessageAware<?>) msg).getBaseConsumerMessage();
			baseMsg.setOnMessageEndTimeMills(System.currentTimeMillis());
		}
	}

	private void setOnMessageStartTime(ConsumerMessage<T> msg) {
		if (msg instanceof BaseConsumerMessageAware) {
			BaseConsumerMessage<?> baseMsg = ((BaseConsumerMessageAware<?>) msg).getBaseConsumerMessage();
			long now = System.currentTimeMillis();
			baseMsg.setOnMessageStartTimeMills(now);

			Transaction latencyT = null;
			if (!msg.isResend()) {
				latencyT = Cat.newTransaction( //
				      CatConstants.TYPE_MESSAGE_CONSUME_LATENCY, msg.getTopic() + ":" + m_groupId);
			} else {
				latencyT = Cat.newTransaction( //
				      CatConstants.TYPE_MESSAGE_CONSUME_RESEND_LATENCY, msg.getTopic() + ":" + m_groupId);
			}
			long delta = System.currentTimeMillis() - baseMsg.getBornTime();

			if (latencyT instanceof DefaultTransaction) {
				((DefaultTransaction) latencyT).setDurationStart(System.nanoTime() - delta * 1000000L);
			}

			latencyT.addData("key", msg.getRefKey());
			latencyT.setStatus(Transaction.SUCCESS);
			latencyT.complete();
		}
	}

	protected abstract void onMessage(ConsumerMessage<T> msg);

}
