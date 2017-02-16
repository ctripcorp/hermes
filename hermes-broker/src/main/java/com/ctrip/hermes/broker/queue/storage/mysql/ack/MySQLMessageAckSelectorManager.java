package com.ctrip.hermes.broker.queue.storage.mysql.ack;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.selector.AbstractSelectorManager;
import com.ctrip.hermes.core.selector.DefaultSelector;
import com.ctrip.hermes.core.selector.OffsetLoader.NoOpOffsetLoader;
import com.ctrip.hermes.core.selector.Selector;
import com.ctrip.hermes.core.selector.Selector.InitialLastUpdateTime;
import com.ctrip.hermes.core.selector.Slot;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.env.config.broker.BrokerConfigProvider;

@Named(type = MySQLMessageAckSelectorManager.class)
public class MySQLMessageAckSelectorManager extends AbstractSelectorManager<String> implements Initializable {
	private static final Logger log = LoggerFactory.getLogger(MySQLMessageAckSelectorManager.class);

	private static final int SLOT_COUNT = 2;

	public static final int SLOT_NORMAL = 0;

	public static final int SLOT_SAFE = 1;

	private static final long ACK_SELECTOR_MIN_FIRE = 10;

	@Inject
	private BrokerConfigProvider m_config;

	private Selector<String> m_selector;

	@Override
	public void initialize() throws InitializationException {
		ThreadPoolExecutor flushExecutor = new ThreadPoolExecutor(m_config.getAckFlushThreadCount(), m_config.getAckFlushThreadCount(), 60, TimeUnit.SECONDS,
		      new LinkedBlockingQueue<Runnable>(), HermesThreadFactory.create("MySQLMessageAckFlushIOThread", true));
		flushExecutor.allowCoreThreadTimeOut(true);
		m_selector = new DefaultSelector<>(flushExecutor, SLOT_COUNT, Long.MAX_VALUE, new NoOpOffsetLoader<String>(), InitialLastUpdateTime.NEWEST);

		HermesThreadFactory.create("MySQLMessageAckSelectorSafeTrigger", true).newThread(new Runnable() {
			@Override
			public void run() {
				while (!Thread.interrupted()) {
					try {
						m_selector.updateAll(false, new Slot(SLOT_SAFE, System.currentTimeMillis(), ACK_SELECTOR_MIN_FIRE));
					} catch (Exception e) {
						log.error("Update MySQL message-ack-selector failed.", e);
					} finally {
						try {
							TimeUnit.MILLISECONDS.sleep(m_config.getAckFlushSelectorSafeTriggerIntervalMillis());
						} catch (InterruptedException e) {
							// ignore it
						}
					}
				}
			}
		}).start();
	}

	@Override
	public Selector<String> getSelector() {
		return m_selector;
	}

	@Override
	protected long nextAwaitingNormalOffset(String key, long doneOffset, Object arg) {
		return doneOffset + m_config.getAckFlushSelectorNormalTriggeringOffsetDeltas((String) key);
	}

	@Override
	protected long nextAwaitingSafeTriggerOffset(String key, Object arg) {
		return System.currentTimeMillis() + m_config.getAckFlushSelectorSafeTriggerTriggeringOffsetDeltas((String) key);
	}

}
