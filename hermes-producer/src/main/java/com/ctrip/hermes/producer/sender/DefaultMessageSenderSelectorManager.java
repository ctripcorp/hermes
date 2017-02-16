/**
 * 
 */
package com.ctrip.hermes.producer.sender;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
import com.ctrip.hermes.core.selector.OffsetLoader.NoOpOffsetLoader;
import com.ctrip.hermes.core.selector.Selector;
import com.ctrip.hermes.core.selector.Selector.InitialLastUpdateTime;
import com.ctrip.hermes.core.selector.Slot;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.producer.config.ProducerConfig;

/**
 * @author marsqing
 *
 *         Jun 28, 2016 4:11:44 PM
 */
@Named(type = MessageSenderSelectorManager.class)
public class DefaultMessageSenderSelectorManager extends AbstractSelectorManager<Pair<String, Integer>> implements
      MessageSenderSelectorManager, Initializable {

	private static Logger log = LoggerFactory.getLogger(DefaultMessageSenderSelectorManager.class);

	@Inject
	private ProducerConfig m_config;

	private Selector<Pair<String, Integer>> m_selector;

	private ExecutorService m_taskExecThreadPool;

	@Override
	public Selector<Pair<String, Integer>> getSelector() {
		return m_selector;
	}

	@Override
	public void initialize() throws InitializationException {
		m_taskExecThreadPool = Executors.newCachedThreadPool(HermesThreadFactory.create("BrokerMessageSender", false));

		// no write from "outside", so write offset never expires
		long maxWriteOffsetTtlMillis = Long.MAX_VALUE;
		NoOpOffsetLoader<Pair<String, Integer>> offsetLoader = new NoOpOffsetLoader<Pair<String, Integer>>();
		m_selector = new DefaultSelector<>(m_taskExecThreadPool, SLOT_COUNT, maxWriteOffsetTtlMillis, offsetLoader,
		      InitialLastUpdateTime.NEWEST);

		Thread safeTriggerThread = HermesThreadFactory.create("MessageSenderSelectorSafeTrigger", true).newThread(
		      new Runnable() {

			      @Override
			      public void run() {
				      while (true) {
					      try {
						      m_selector.updateAll(false, new Slot(SLOT_SAFE_TRIGGER_INDEX, System.currentTimeMillis(),
						            m_config.getMessageSenderSelectorSafeTriggerMinFireIntervalMillis()));
					      } catch (Throwable e) {
						      log.error("Error update MessageSenderSelectorSafeTrigger", e);
					      } finally {
						      try {
							      TimeUnit.MILLISECONDS.sleep(m_config.getMessageSenderSelectorSafeTriggerIntervalMillis());
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
		return doneOffset + m_config.getMessageSenderSelectorNormalTriggeringOffsetDelta(topic);
	}

	@Override
	protected long nextAwaitingSafeTriggerOffset(Pair<String, Integer> key, Object arg) {
		String topic = key.getKey();
		return System.currentTimeMillis() + m_config.getMessageSenderSelectorSafeTriggerTriggeringOffsetDelta(topic);
	}

}
