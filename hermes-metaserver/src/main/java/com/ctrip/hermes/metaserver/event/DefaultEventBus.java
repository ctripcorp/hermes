package com.ctrip.hermes.metaserver.event;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.metaserver.cluster.ClusterStateHolder;
import com.ctrip.hermes.metaserver.event.EventHandlerRegistry.Function;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = EventBus.class)
public class DefaultEventBus implements EventBus, Initializable {

	private static final Logger log = LoggerFactory.getLogger(DefaultEventBus.class);

	@Inject
	private EventHandlerRegistry m_handlerRegistry;

	private ExecutorService m_executor;

	@Override
	public void pubEvent(final Event event) {
		final EventType type = event.getType();

		List<EventHandler> handlers = m_handlerRegistry.findHandler(type);

		if (handlers == null || handlers.isEmpty()) {
			log.error("Event handler not found for type {}.", type);
		} else {
			for (final EventHandler handler : handlers) {
				m_executor.submit(new Runnable() {

					@Override
					public void run() {

						try {
							new VersionGuardedTask(event.getVersion()) {

								@Override
								public String name() {
									return handler.getName();
								}

								@Override
								protected void doRun() throws Exception {
									handler.onEvent(event);
								}
							}.run();
						} catch (Exception e) {
							log.error("Exception occurred while processing event {} in handler {}", event.getType(),
							      handler.getName(), e);
						}
					}
				});
			}
		}

	}

	@Override
	public void initialize() throws InitializationException {
		m_executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<Runnable>(),
		      HermesThreadFactory.create("EventBusExecutor", true));
	}

	@Override
	public ExecutorService getExecutor() {
		return m_executor;
	}

	@Override
	public void start(final ClusterStateHolder clusterStateHolder) {
		m_handlerRegistry.forEachHandler(new Function() {

			@Override
			public void apply(EventHandler handler) {
				handler.setClusterStateHolder(clusterStateHolder);
				handler.setEventBus(DefaultEventBus.this);
				handler.start();
			}
		});
	}

}
