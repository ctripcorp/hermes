/**
 * 
 */
package com.ctrip.hermes.broker.selector;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.selector.AbstractSelectorManager;
import com.ctrip.hermes.core.selector.DefaultSelector;
import com.ctrip.hermes.core.selector.OffsetLoader;
import com.ctrip.hermes.core.selector.OffsetLoader.NoOpOffsetLoader;
import com.ctrip.hermes.core.selector.Selector;
import com.ctrip.hermes.core.selector.Selector.InitialLastUpdateTime;
import com.ctrip.hermes.core.selector.Slot;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.env.config.broker.BrokerConfigProvider;

/**
 * @author marsqing
 *
 *         Jun 27, 2016 3:06:10 PM
 */
@Named(type = SendMessageSelectorManager.class)
public class DefaultSendMessageSelectorManager extends AbstractSelectorManager<Pair<String, Integer>> implements
      SendMessageSelectorManager, Initializable {

	private static Logger log = LoggerFactory.getLogger(DefaultSendMessageSelectorManager.class);

	@Inject
	private BrokerConfigProvider m_config;

	private DefaultSelector<Pair<String, Integer>> m_selector;

	@Override
	public Selector<Pair<String, Integer>> getSelector() {
		return m_selector;
	}

	@Override
	public void initialize() throws InitializationException {
		ThreadPoolExecutor flushExecutor = new ThreadPoolExecutor(m_config.getMessageQueueFlushThreadCount(),
		      m_config.getMessageQueueFlushThreadCount(), 60, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
		      HermesThreadFactory.create("MessageQueueFlushExecutor", true));
		flushExecutor.allowCoreThreadTimeOut(true);

		// send message update never expires
		long maxWriteOffsetTtlMillis = Long.MAX_VALUE;
		OffsetLoader<Pair<String, Integer>> offsetLoader = new NoOpOffsetLoader<Pair<String, Integer>>();

		m_selector = new DefaultSelector<>(flushExecutor, SLOT_COUNT, maxWriteOffsetTtlMillis, offsetLoader,
		      InitialLastUpdateTime.NEWEST);

		Thread safeTriggerThread = HermesThreadFactory.create("SendMessageSelectorSafeTrigger", true).newThread(
		      new Runnable() {

			      @Override
			      public void run() {
				      while (true) {
					      try {
						      m_selector.updateAll(false, new Slot(SLOT_SAFE_TRIGGER_INDEX, System.currentTimeMillis(),
						            m_config.getSendMessageSelectorSafeTriggerMinFireIntervalMillis()));
					      } catch (Throwable e) {
						      log.error("Error update SendMessageSelectorSafeTrigger", e);
					      } finally {
						      try {
							      TimeUnit.MILLISECONDS.sleep(m_config.getSendMessageSelectorSafeTriggerIntervalMillis());
						      } catch (InterruptedException e) {
							      // ignore
						      }
					      }
				      }
			      }
		      });

		safeTriggerThread.start();
	}

	@Override
	protected long nextAwaitingNormalOffset(Pair<String, Integer> key, long doneOffset, Object arg) {
		String topic = key.getKey();
		return doneOffset + m_config.getSendMessageSelectorNormalTriggeringOffsetDelta(topic);
	}

	@Override
	protected long nextAwaitingSafeTriggerOffset(Pair<String, Integer> key, Object arg) {
		String topic = key.getKey();
		return System.currentTimeMillis() + m_config.getSendMessageSelectorSafeTriggerTriggeringOffsetDelta(topic);
	}

}
