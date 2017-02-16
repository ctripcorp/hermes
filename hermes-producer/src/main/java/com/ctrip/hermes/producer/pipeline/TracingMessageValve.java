package com.ctrip.hermes.producer.pipeline;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.net.Networks;

import com.ctrip.hermes.core.constants.CatConstants;
import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.pipeline.PipelineContext;
import com.ctrip.hermes.core.pipeline.spi.Valve;
import com.ctrip.hermes.producer.config.ProducerConfig;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Event;
import com.dianping.cat.message.spi.MessageTree;

@Named(type = Valve.class, value = TracingMessageValve.ID)
public class TracingMessageValve implements Valve {
	public static final String ID = "tracing";

	@Inject
	private MetaService m_metaService;

	@Inject
	private ProducerConfig m_config;

	@Override
	public void handle(PipelineContext<?> ctx, Object payload) {
		if (m_config.isCatEnabled()) {
			ProducerMessage<?> msg = (ProducerMessage<?>) payload;
			String topic = msg.getTopic();

			boolean connectCatTransactions = m_metaService.findTopicByName(topic).isConnectCatTransactions();

			try {
				String ip = Networks.forIp().getLocalHostAddress();
				Cat.logEvent("Message:" + topic, "Produced:" + ip, Event.SUCCESS, "key=" + msg.getKey());
				Cat.logEvent("Producer:" + ip, topic, Event.SUCCESS, "key=" + msg.getKey());

				if (msg.isWithCatTrace() && connectCatTransactions) {
					MessageTree tree = Cat.getManager().getThreadLocalMessageTree();
					String childMsgId = Cat.createMessageId();
					String rootMsgId = tree.getRootMessageId();
					String msgId = Cat.getCurrentMessageId();
					rootMsgId = rootMsgId == null ? msgId : rootMsgId;

					msg.addDurableSysProperty(CatConstants.CURRENT_MESSAGE_ID, msgId);
					msg.addDurableSysProperty(CatConstants.SERVER_MESSAGE_ID, childMsgId);
					msg.addDurableSysProperty(CatConstants.ROOT_MESSAGE_ID, rootMsgId);
					Cat.logEvent(CatConstants.TYPE_REMOTE_CALL, "", Event.SUCCESS, childMsgId);
				}
				ctx.next(payload);

			} catch (Exception e) {
				Cat.logError(e);
				throw new RuntimeException(e);
			}
		} else {
			try {
				ctx.next(payload);
			} catch (Exception e) {
				Cat.logError(e);
				throw new RuntimeException(e);
			}
		}
	}

}
