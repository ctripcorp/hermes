package com.ctrip.hermes.core.transport.command.processor;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.config.CoreConfig;
import com.ctrip.hermes.core.constants.CatConstants;
import com.ctrip.hermes.core.status.StatusMonitor;
import com.ctrip.hermes.core.transport.ManualRelease;
import com.ctrip.hermes.core.transport.command.Command;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.dianping.cat.Cat;

@Named(type = CommandProcessorManager.class)
public class CommandProcessorManager implements Initializable {
	private static final Logger log = LoggerFactory.getLogger(CommandProcessorManager.class);

	@Inject
	private CommandProcessorRegistry m_registry;

	@Inject
	private CoreConfig m_config;

	private Map<CommandProcessor, ExecutorService> m_executors = new ConcurrentHashMap<CommandProcessor, ExecutorService>();

	private AtomicBoolean m_stopped = new AtomicBoolean(false);

	public void offer(final CommandProcessorContext ctx) {
		if (m_stopped.get()) {
			return;
		}

		Command cmd = ctx.getCommand();
		final CommandType type = cmd.getHeader().getType();
		final CommandProcessor processor = m_registry.findProcessor(type);
		if (processor == null) {
			log.error("Command processor not found for type {}", type);
		} else {
			StatusMonitor.INSTANCE.commandReceived(type, ctx.getRemoteIp());

			ExecutorService executorService = m_executors.get(processor);

			if (executorService == null) {
				throw new IllegalArgumentException(String.format("No executor associated to processor %s", processor
				      .getClass().getSimpleName()));
			} else {
				executorService.submit(new Runnable() {

					@Override
					public void run() {
						Command cmd = ctx.getCommand();
						if (cmd.getReceiveTime() > 0
						      && (System.currentTimeMillis() - cmd.getReceiveTime()) > m_config
						            .getCommandProcessorCmdExpireMillis()) {
							Cat.logEvent(CatConstants.TYPE_CMD_DROP, cmd.getHeader().getType().toString());

							if (cmd.getClass().isAnnotationPresent(ManualRelease.class)) {
								cmd.release();
							}

							return;
						}

						long start = System.currentTimeMillis();
						try {
							processor.process(ctx);
						} catch (Exception e) {
							StatusMonitor.INSTANCE.commandProcessorException(type, processor);
							log.error("Exception occurred while process command.", e);
						} finally {
							StatusMonitor.INSTANCE.commandProcessed(type, (System.currentTimeMillis() - start));
						}
					}
				});
			}
		}
	}

	@Override
	public void initialize() throws InitializationException {
		Set<CommandProcessor> cmdProcessors = m_registry.listAllProcessors();

		for (CommandProcessor cmdProcessor : cmdProcessors) {

			String threadNamePrefix = String.format("CmdProcessor-%s", cmdProcessor.getClass().getSimpleName());
			ExecutorService executor = null;
			BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>();

			int threadCount = m_config.getCommandProcessorDefaultThreadCount();

			if (cmdProcessor.getClass().isAnnotationPresent(ThreadCount.class)) {
				threadCount = cmdProcessor.getClass().getAnnotation(ThreadCount.class).value();
			}

			executor = new ThreadPoolExecutor(threadCount, threadCount, 0L, TimeUnit.MILLISECONDS, workQueue,
			      HermesThreadFactory.create(threadNamePrefix, false));

			m_executors.put(cmdProcessor, executor);
			StatusMonitor.INSTANCE.watchCommandProcessorQueue(threadNamePrefix, workQueue);
		}

	}

	public void stop() {
		if (m_stopped.compareAndSet(false, true)) {
			for (ExecutorService executor : m_executors.values()) {
				executor.shutdown();
			}
		}
	}
}
