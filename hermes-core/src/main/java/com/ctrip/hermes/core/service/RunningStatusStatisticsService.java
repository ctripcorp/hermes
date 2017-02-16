package com.ctrip.hermes.core.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.config.CoreConfig;
import com.ctrip.hermes.core.utils.HermesThreadFactory;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = RunningStatusStatisticsService.class)
public class RunningStatusStatisticsService implements Initializable {
	private static final Logger log = LoggerFactory.getLogger(RunningStatusStatisticsService.class);

	@Inject
	private CoreConfig m_config;

	@Override
	public void initialize() throws InitializationException {
		String runningStatusStatSwitch = System.getProperty("runningStatusStat", "false");
		if ("true".equalsIgnoreCase(runningStatusStatSwitch)) {
			Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create("RunningStatusStat", true))
			      .scheduleWithFixedDelay(new Runnable() {

				      @Override
				      public void run() {

					      try {
						      printAllThreads();
					      } catch (Exception e) {
						      // ignore it
					      }

				      }
			      }, 0, m_config.getRunningStatusStatInterval(), TimeUnit.SECONDS);
		}

	}

	private void printAllThreads() {
		Map<Thread, StackTraceElement[]> allStackTraces = Thread.getAllStackTraces();
		Map<String, List<Pair<String, Boolean>>> groups = new HashMap<String, List<Pair<String, Boolean>>>();
		for (Thread thread : allStackTraces.keySet()) {
			String group = thread.getThreadGroup().getName();
			if (!groups.containsKey(group)) {
				groups.put(group, new ArrayList<Pair<String, Boolean>>());
			}
			groups.get(group).add(new Pair<String, Boolean>(thread.getName(), thread.isDaemon()));
		}

		log.info("Running threads({})", JSON.toJSONString(groups));
	}
}
