package com.ctrip.hermes.consumer.engine.config;

import java.io.IOException;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.env.ClientEnvironment;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = ConsumerConfig.class)
public class ConsumerConfig implements Initializable {

	public static final int DEFAULT_LOCALCACHE_SIZE = 200;

	public static final int DEFAULT_MAX_ACK_HOLDER_SIZE = 50000;

	private static final int DEFAULT_NOTIFIER_THREAD_COUNT = 1;

	private static final int DEFAULT_NOTIFIER_WORK_QUEUE_SIZE = 1;

	private static final int DEFAULT_ACK_CHECKER_INTERVAL_MILLIS = 1000;

	private static final int DEFAULT_ACK_CHECKER_IO_THREAD_COUNT = 3;

	private static final int DEFAULT_ACK_CHECKER_RESULT_TIMEOUT_MILLIS = 4000;

	private static final int DEFAULT_ACK_CHECKER_ACCEPT_TIMEOUT_MILLIS = 4000;

	private static final int DEFAULT_PULL_MESSAGE_ACCEPT_TIMEOUT_MILLIS = 4000;

	private static final int DEFAULT_QUERY_OFFSET_ACCEPT_TIMEOUT_MILLIS = 2000;

	private static final int DEFAULT_NO_MESSAGE_WAIT_BASE_MILLIS = 10;

	private static final int DEFAULT_NO_MESSAGE_WAIT_MAX_MILLIS = 10;

	private static final int DEFAULT_ACK_COMMAND_MAX_SIZE = 5000;

	private static final long DEFAULT_RENEW_LEASE_TIME_MILLIS_BEFORE_EXPIRED = 5000L;

	private static final long DEFAULT_STOP_CONSUMER_TIME_MILLIS_BEFORE_LEASE_EXPIRED = 1000L;

	@Inject
	private ClientEnvironment m_env;

	@Inject
	private MetaService m_metaService;

	private int m_ackCheckerIntervalMillis = DEFAULT_ACK_CHECKER_INTERVAL_MILLIS;

	private int m_ackCheckerIoThreadCount = DEFAULT_ACK_CHECKER_IO_THREAD_COUNT;

	private int m_ackCheckerResultTimeoutMillis = DEFAULT_ACK_CHECKER_RESULT_TIMEOUT_MILLIS;

	private int m_ackCheckerAcceptTimeoutMillis = DEFAULT_ACK_CHECKER_ACCEPT_TIMEOUT_MILLIS;

	private int m_noMessageWaitBaseMillis = DEFAULT_NO_MESSAGE_WAIT_BASE_MILLIS;

	private int m_noMessageWaitMaxMillis = DEFAULT_NO_MESSAGE_WAIT_MAX_MILLIS;

	private int m_ackCommandMaxSize = DEFAULT_ACK_COMMAND_MAX_SIZE;

	private int m_pullMessageAcceptTimeoutMillis = DEFAULT_PULL_MESSAGE_ACCEPT_TIMEOUT_MILLIS;

	private int m_queryOffsetAcceptTimeoutMillis = DEFAULT_QUERY_OFFSET_ACCEPT_TIMEOUT_MILLIS;

	private long m_renewLeaseTimeMillisBeforeExpired = DEFAULT_RENEW_LEASE_TIME_MILLIS_BEFORE_EXPIRED;

	private long m_stopConsumerTimeMillisBeforeLeaseExpired = DEFAULT_STOP_CONSUMER_TIME_MILLIS_BEFORE_LEASE_EXPIRED;

	private int m_kafkaPollFailWaitBase = 500;

	private int m_kafkaPollFailWaitMax = 5000;

	public int getLocalCacheSize(String topic) throws IOException {
		String localCacheSizeStr = m_env.getConsumerConfig(topic).getProperty("consumer.localcache.size");

		if (StringUtils.isNumeric(localCacheSizeStr)) {
			return Integer.valueOf(localCacheSizeStr);
		}

		return DEFAULT_LOCALCACHE_SIZE;
	}

	public long getRenewLeaseTimeMillisBeforeExpired() {
		return m_renewLeaseTimeMillisBeforeExpired;
	}

	public long getStopConsumerTimeMillsBeforLeaseExpired() {
		return m_stopConsumerTimeMillisBeforeLeaseExpired;
	}

	public long getDefaultLeaseAcquireDelayMillis() {
		return 500L;
	}

	public long getDefaultLeaseRenewDelayMillis() {
		return 500L;
	}

	public int getNoMessageWaitBaseMillis() {
		return m_noMessageWaitBaseMillis;
	}

	public int getNoMessageWaitMaxMillis() {
		return m_noMessageWaitMaxMillis;
	}

	public int getNoEndpointWaitBaseMillis() {
		return 500;
	}

	public int getNoEndpointWaitMaxMillis() {
		return 4000;
	}

	public int getNotifierThreadCount(String topic) throws IOException {
		String threadCountStr = m_env.getConsumerConfig(topic).getProperty("consumer.notifier.threadcount");
		if (StringUtils.isNumeric(threadCountStr)) {
			return Integer.valueOf(threadCountStr);
		}

		return DEFAULT_NOTIFIER_THREAD_COUNT;
	}

	public long getQueryOffsetTimeoutMillis() {
		return 3000;
	}

	public int getPullMessageAcceptTimeoutMillis() {
		return m_pullMessageAcceptTimeoutMillis;
	}

	public int getQueryOffsetAcceptTimeoutMillis() {
		return m_queryOffsetAcceptTimeoutMillis;
	}

	public int getMaxAckHolderSize(String topicName) throws IOException {
		String maxAckHolderSizeStr = m_env.getConsumerConfig(topicName).getProperty("consumer.max.ack.holder.size");

		if (StringUtils.isNumeric(maxAckHolderSizeStr)) {
			return Integer.valueOf(maxAckHolderSizeStr);
		}

		return DEFAULT_MAX_ACK_HOLDER_SIZE;
	}

	@Override
	public void initialize() throws InitializationException {
		String ackCheckerIntervalMillis = m_env.getGlobalConfig().getProperty("consumer.ack.checker.interval.millis");
		if (StringUtils.isNumeric(ackCheckerIntervalMillis)) {
			m_ackCheckerIntervalMillis = Integer.valueOf(ackCheckerIntervalMillis);
		}

		String ackCheckerIoThreadCount = m_env.getGlobalConfig().getProperty("consumer.ack.checker.io.thread.count");
		if (StringUtils.isNumeric(ackCheckerIoThreadCount)) {
			m_ackCheckerIoThreadCount = Integer.valueOf(ackCheckerIoThreadCount);
		}

		String ackCheckerResultTimeoutMillis = m_env.getGlobalConfig().getProperty(
		      "consumer.ack.checker.result.timeout.millis");
		if (StringUtils.isNumeric(ackCheckerResultTimeoutMillis)) {
			m_ackCheckerResultTimeoutMillis = Integer.valueOf(ackCheckerResultTimeoutMillis);
		}

		String noMessageWaitBaseMillis = m_env.getGlobalConfig().getProperty("consumer.no.message.wait.base.millis");
		if (StringUtils.isNumeric(noMessageWaitBaseMillis)) {
			m_noMessageWaitBaseMillis = Integer.valueOf(noMessageWaitBaseMillis);
		}

		String noMessageWaitMaxMillis = m_env.getGlobalConfig().getProperty("consumer.no.message.wait.max.millis");
		if (StringUtils.isNumeric(noMessageWaitMaxMillis)) {
			m_noMessageWaitMaxMillis = Integer.valueOf(noMessageWaitMaxMillis);
		}

		String maxAckCmdSize = m_env.getGlobalConfig().getProperty("consumer.ack.max.cmd.size");
		if (StringUtils.isNumeric(maxAckCmdSize)) {
			m_ackCommandMaxSize = Integer.valueOf(maxAckCmdSize);
		}
		String ackAcceptTimeoutMillis = m_env.getGlobalConfig().getProperty("consumer.ack.accept.timeout.millis");
		if (StringUtils.isNumeric(ackAcceptTimeoutMillis)) {
			m_ackCheckerAcceptTimeoutMillis = Integer.valueOf(ackAcceptTimeoutMillis);
		}
		String pullMessageAcceptTimeoutMillis = m_env.getGlobalConfig().getProperty(
		      "consumer.pull.message.accept.timeout.millis");
		if (StringUtils.isNumeric(pullMessageAcceptTimeoutMillis)) {
			m_pullMessageAcceptTimeoutMillis = Integer.valueOf(pullMessageAcceptTimeoutMillis);
		}
		String queryOffsetAcceptTimeoutMillis = m_env.getGlobalConfig().getProperty(
		      "consumer.query.offset.accept.timeout.millis");
		if (StringUtils.isNumeric(queryOffsetAcceptTimeoutMillis)) {
			m_queryOffsetAcceptTimeoutMillis = Integer.valueOf(queryOffsetAcceptTimeoutMillis);
		}
		String renewLeaseTimeMillisBeforeExpired = m_env.getGlobalConfig().getProperty(
		      "consumer.renew.lease.time.before.expired.millis");
		if (StringUtils.isNumeric(renewLeaseTimeMillisBeforeExpired)) {
			m_renewLeaseTimeMillisBeforeExpired = Integer.valueOf(renewLeaseTimeMillisBeforeExpired);
		}
		String stopConsumerTimeMillisBeforeLeaseExpired = m_env.getGlobalConfig().getProperty(
		      "consumer.stop.consumer.before.lease.expired.millis");
		if (StringUtils.isNumeric(stopConsumerTimeMillisBeforeLeaseExpired)) {
			m_stopConsumerTimeMillisBeforeLeaseExpired = Integer.valueOf(stopConsumerTimeMillisBeforeLeaseExpired);
		}
	}

	public int getAckCheckerIntervalMillis() {
		return m_ackCheckerIntervalMillis;
	}

	public int getAckCheckerIoThreadCount() {
		return m_ackCheckerIoThreadCount;
	}

	public int getAckCheckerResultTimeoutMillis() {
		return m_ackCheckerResultTimeoutMillis;
	}

	public int getAckCheckerAcceptTimeoutMillis() {
		return m_ackCheckerAcceptTimeoutMillis;
	}

	public int getNotifierWorkQueueSize(String topic) throws IOException {
		String workQueueSizeStr = m_env.getConsumerConfig(topic).getProperty("consumer.notifier.work.queue.size");
		if (StringUtils.isNumeric(workQueueSizeStr)) {
			return Integer.valueOf(workQueueSizeStr);
		}
		return DEFAULT_NOTIFIER_WORK_QUEUE_SIZE;
	}

	public int getAckCommandMaxSize() {
		return m_ackCommandMaxSize;
	}

	public int getKafkaPollFailWaitBase() {
		return m_kafkaPollFailWaitBase;
	}

	public void setKafkaPollFailWaitBase(int kafkaPollFailWaitBase) {
		this.m_kafkaPollFailWaitBase = kafkaPollFailWaitBase;
	}

	public int getKafkaPollFailWaitMax() {
		return m_kafkaPollFailWaitMax;
	}

	public void setKafkaPollFailWaitMax(int kafkaPollFailWaitMax) {
		this.m_kafkaPollFailWaitMax = kafkaPollFailWaitMax;
	}

}
