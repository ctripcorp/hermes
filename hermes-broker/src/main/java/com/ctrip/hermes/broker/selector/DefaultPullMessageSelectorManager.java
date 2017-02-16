/**
 * 
 */
package com.ctrip.hermes.broker.selector;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.broker.dal.hermes.MessagePriority;
import com.ctrip.hermes.broker.dal.hermes.MessagePriorityDao;
import com.ctrip.hermes.broker.dal.hermes.MessagePriorityEntity;
import com.ctrip.hermes.core.bo.Tp;
import com.ctrip.hermes.core.selector.AbstractSelectorManager;
import com.ctrip.hermes.core.selector.DefaultSelector;
import com.ctrip.hermes.core.selector.OffsetLoader;
import com.ctrip.hermes.core.selector.Selector;
import com.ctrip.hermes.core.selector.Selector.InitialLastUpdateTime;
import com.ctrip.hermes.core.selector.Slot;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.env.config.broker.BrokerConfigProvider;

/**
 * @author marsqing
 *
 *         Jun 22, 2016 3:54:28 PM
 */
@Named(type = PullMessageSelectorManager.class)
public class DefaultPullMessageSelectorManager extends AbstractSelectorManager<Tp> implements Initializable,
      PullMessageSelectorManager {

	private static Logger log = LoggerFactory.getLogger(DefaultPullMessageSelectorManager.class);

	@Inject
	private BrokerConfigProvider m_config;

	@Inject
	private MessagePriorityDao messageDao;

	private ExecutorService longPollingThreadPool;

	private ThreadPoolExecutor offsetLoaderThreadPool;

	private Selector<Tp> selector;

	@Override
	public void initialize() throws InitializationException {
		longPollingThreadPool = Executors.newFixedThreadPool(m_config.getLongPollingServiceThreadCount(),
		      HermesThreadFactory.create("LongPollingService", true));

		offsetLoaderThreadPool = new ThreadPoolExecutor(m_config.getPullMessageSelectorOffsetLoaderThreadPoolSize(),
		      m_config.getPullMessageSelectorOffsetLoaderThreadPoolSize(),
		      m_config.getPullMessageSelectorOffsetLoaderThreadPoolKeepaliveSeconds(), TimeUnit.SECONDS,
		      new LinkedBlockingQueue<Runnable>(), HermesThreadFactory.create("SelectorOffsetLoader", false));
		offsetLoaderThreadPool.allowCoreThreadTimeOut(true);

		OffsetLoader<Tp> offsetLoader = new DaoOffsetLoader();

		selector = new DefaultSelector<>(longPollingThreadPool, SLOT_COUNT,
		      m_config.getPullMessageSelectorWriteOffsetTtlMillis(), offsetLoader, InitialLastUpdateTime.OLDEST);

		Thread safeTriggerThread = HermesThreadFactory.create("PullMessageSelectorSafeTrigger", true).newThread(
		      new Runnable() {

			      @Override
			      public void run() {
				      while (true) {
					      try {
						      selector.updateAll(
						            false,
						            new Slot(SLOT_RESEND_INDEX, System.currentTimeMillis(), m_config
						                  .getPullMessageSelectorSafeTriggerMinFireIntervalMillis()));
					      } catch (Throwable e) {
						      log.error("Error update PullMessageSelectorSafeTrigger", e);
					      } finally {
						      try {
							      TimeUnit.MILLISECONDS.sleep(m_config.getPullMessageSelectorSafeTriggerIntervalMillis());
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
	public Selector<Tp> getSelector() {
		return selector;
	}

	class DaoOffsetLoader implements OffsetLoader<Tp> {
		private void doLoadAsync(final Tp tp, final int priority, final Selector<Tp> selector) {
			offsetLoaderThreadPool.submit(new Runnable() {

				@Override
				public void run() {
					try {
						List<MessagePriority> offsets = messageDao.findLatestOffset(tp.getTopic(), tp.getPartition(),
						      priority, MessagePriorityEntity.READSET_OFFSET);
						if (offsets != null && offsets.size() > 0) {
							MessagePriority offset = offsets.get(0);
							int index = priority == 0 ? SLOT_PRIORITY_INDEX : SLOT_NONPRIORITY_INDEX;
							selector.update(tp, true, new Slot(index, offset.getId()));
							log.info("Loaded max offset:{} for topic:{} partition:{}, priority:{}", offset.getId(),
							      tp.getTopic(), tp.getPartition(), priority);
						}
					} catch (Exception e) {
						log.error("Error load max id for topic:{} partition:{} priority:{} for selector", tp.getTopic(),
						      tp.getPartition(), priority, e);
					}
				}

			});
		}

		@Override
		public void loadAsync(final Tp tp, final Selector<Tp> selector) {
			doLoadAsync(tp, 0, selector);
			doLoadAsync(tp, 1, selector);
		}
	}

	@Override
	protected long nextAwaitingNormalOffset(Tp tp, long doneOffset, Object arg) {
		String groupId = (String) arg;
		return doneOffset + m_config.getPullMessageSelectorNormalTriggeringOffsetDelta(tp.getTopic(), groupId);
	}

	@Override
	protected long nextAwaitingSafeTriggerOffset(Tp tp, Object arg) {
		String groupId = (String) arg;
		return System.currentTimeMillis()
		      + m_config.getPullMessageSelectorSafeTriggerTriggeringOffsetDelta(tp.getTopic(), groupId);
	}

}
