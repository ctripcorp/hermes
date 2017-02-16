package com.ctrip.hermes.core.config;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.env.ClientEnvironment;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = CoreConfig.class)
public class CoreConfig implements Initializable {

	private static final int DEFAULT_CHANNEL_READ_IDLE_TIME_SECONDS = 60;

	private static final int DEFAULT_CHANNEL_WRITE_IDLE_TIME_SECONDS = 60;

	private static final int DEFAULT_CHANNEL_ALL_IDLE_TIME_SECONDS = 60;

	private static final int DEFAULT_MAX_CLIENT_TIME_DIFF_MILLIS = 2000;

	private static final int DEFAULT_COMMAND_PROCESSOR_DEFAULT_THREAD_COUNT = 3;

	public static final String TIME_UNSYNC_HEADER = "X-Hermes-Time-Unsync";

	private static final long DEFAULT_META_CACHE_REFRESH_INTERVAL_SECOND = 60 * 2;

	private static final long DEFAULT_COMMAND_PROCESSOR_CMD_EXPIRE_MILLIS = 60 * 1000;

	private static final long DEFAULT_ENDPOINT_REFRESH_MIN_INTERVAL_MILLIS = 10 * 1000;

	private static final long DEFAULT_ENDPOINT_CACHE_MILLIS = 2 * 60 * 1000;

	@Inject
	private ClientEnvironment m_env;

	private int m_channelReadIdle = DEFAULT_CHANNEL_READ_IDLE_TIME_SECONDS;

	private int m_channelWriteIdle = DEFAULT_CHANNEL_WRITE_IDLE_TIME_SECONDS;

	private int m_channelAllIdle = DEFAULT_CHANNEL_ALL_IDLE_TIME_SECONDS;

	private long m_maxClientTimeDiffMillis = DEFAULT_MAX_CLIENT_TIME_DIFF_MILLIS;

	private int m_commandProcessorDefaultThreadCount = DEFAULT_COMMAND_PROCESSOR_DEFAULT_THREAD_COUNT;

	private long m_metaCacheRefreshIntervalSecond = DEFAULT_META_CACHE_REFRESH_INTERVAL_SECOND;

	private long m_commandProcessorCmdExpireMillis = DEFAULT_COMMAND_PROCESSOR_CMD_EXPIRE_MILLIS;

	private long m_endpointRefreshMinIntervalMillis = DEFAULT_ENDPOINT_REFRESH_MIN_INTERVAL_MILLIS;

	private long m_endpointCacheMillis = DEFAULT_ENDPOINT_CACHE_MILLIS;

	@Override
	public void initialize() throws InitializationException {
		String metaRefreshInterval = m_env.getGlobalConfig().getProperty("meta.refresh.interval.seconds");
		if (StringUtils.isNumeric(metaRefreshInterval)) {
			m_metaCacheRefreshIntervalSecond = Integer.valueOf(metaRefreshInterval);
		}

		String endpointRefreshInterval = m_env.getGlobalConfig().getProperty("endpoint.refresh.interval.millis");
		if (StringUtils.isNumeric(endpointRefreshInterval)) {
			m_endpointRefreshMinIntervalMillis = Integer.valueOf(endpointRefreshInterval);
		}

		String endpointCacheMillis = m_env.getGlobalConfig().getProperty("endpoint.cache.millis");
		if (StringUtils.isNumeric(endpointCacheMillis)) {
			m_endpointCacheMillis = Integer.valueOf(endpointCacheMillis);
		}

		String readIdleStr = m_env.getGlobalConfig().getProperty("channel.read.idle.seconds");
		if (StringUtils.isNumeric(readIdleStr)) {
			m_channelReadIdle = Integer.valueOf(readIdleStr);
		}
		String writeIdle = m_env.getGlobalConfig().getProperty("channel.write.idle.seconds");
		if (StringUtils.isNumeric(writeIdle)) {
			m_channelWriteIdle = Integer.valueOf(writeIdle);
		}

		String allIdleStr = m_env.getGlobalConfig().getProperty("channel.all.idle.seconds");
		if (StringUtils.isNumeric(allIdleStr)) {
			m_channelAllIdle = Integer.valueOf(allIdleStr);
		}

		String maxClientTimeDiffMillis = m_env.getGlobalConfig().getProperty("max.client.time.diff.millis");
		if (StringUtils.isNumeric(maxClientTimeDiffMillis)) {
			m_maxClientTimeDiffMillis = Long.valueOf(maxClientTimeDiffMillis);
		}

		String commandProcessorDefaultThreadCountStr = m_env.getGlobalConfig().getProperty(
		      "command.processor.default.thread.count");
		if (StringUtils.isNumeric(commandProcessorDefaultThreadCountStr)) {
			m_commandProcessorDefaultThreadCount = Integer.valueOf(commandProcessorDefaultThreadCountStr);
		}
		String commandProcessorCmdExpireMillisStr = m_env.getGlobalConfig().getProperty(
		      "command.processor.cmd.expire.millis");
		if (StringUtils.isNumeric(commandProcessorCmdExpireMillisStr)) {
			m_commandProcessorCmdExpireMillis = Integer.valueOf(commandProcessorCmdExpireMillisStr);
		}

	}

	public int getCommandProcessorDefaultThreadCount() {
		return m_commandProcessorDefaultThreadCount;
	}

	public int getMetaServerIpFetchInterval() {
		return 5;
	}

	public int getMetaServerConnectTimeout() {
		return 2000;
	}

	public int getMetaServerReadTimeout() {
		return 5000;
	}

	public long getRunningStatusStatInterval() {
		return 30;
	}

	public long getMetaCacheRefreshIntervalSecond() {
		return m_metaCacheRefreshIntervalSecond;
	}

	public int getNettySendBufferSize() {
		return 65535;
	}

	public int getNettyReceiveBufferSize() {
		return 65535;
	}

	public long getEndpointChannelAutoReconnectDelay() {
		return 1;
	}

	public int getEndpointChannelMaxIdleTime() {
		return m_channelAllIdle;
	}

	public int getEndpointChannelReadIdleTime() {
		return m_channelReadIdle;
	}

	public int getEndpointChannelWriteIdleTime() {
		return m_channelWriteIdle;
	}

	public String getAvroSchemaRetryUrlKey() {
		return "schema.registry.url";
	}

	public long getMaxClientTimeDiffMillis() {
		return m_maxClientTimeDiffMillis;
	}

	public long getCMessagingConfigUpdateInterval() {
		return 5000;
	}

	public long getCommandProcessorCmdExpireMillis() {
		return m_commandProcessorCmdExpireMillis;
	}

	public long getEndpointRefreshMinIntervalMillis() {
		return m_endpointRefreshMinIntervalMillis;
	}

	public long getEndpointCacheMillis() {
		return m_endpointCacheMillis;
	}

}
