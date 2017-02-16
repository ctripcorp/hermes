package com.ctrip.hermes.core.transport.monitor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.helper.Files.IO;
import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.meta.manual.ManualConfig;
import com.ctrip.hermes.core.transport.command.v6.FetchManualConfigCommandV6;
import com.ctrip.hermes.core.transport.command.v6.FetchManualConfigResultCommandV6;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = FetchManualConfigResultMonitor.class)
public class DefaultFetchManualConfigResultMonitor implements FetchManualConfigResultMonitor {
	private static final Logger log = LoggerFactory.getLogger(DefaultFetchManualConfigResultMonitor.class);

	private Map<Long, SettableFuture<ManualConfig>> m_cmds = new ConcurrentHashMap<>();

	private ReentrantLock m_lock = new ReentrantLock();

	@Override
	public Future<ManualConfig> monitor(FetchManualConfigCommandV6 cmd) {
		m_lock.lock();
		try {
			SettableFuture<ManualConfig> future = SettableFuture.create();
			m_cmds.put(cmd.getHeader().getCorrelationId(), future);
			return future;
		} finally {
			m_lock.unlock();
		}
	}

	@Override
	public void resultReceived(FetchManualConfigResultCommandV6 resultCmd) {
		if (resultCmd != null) {
			SettableFuture<ManualConfig> future = null;
			m_lock.lock();
			try {
				future = m_cmds.remove(resultCmd.getHeader().getCorrelationId());
			} finally {
				m_lock.unlock();
			}
			if (future != null) {
				try {
					byte[] data = resultCmd.getData();
					if (data != null && data.length > 0) {
						future.set(decompress(data));
					} else {
						future.set(null);
					}
				} catch (Exception e) {
					log.warn("Exception occurred while calling resultReceived", e);
				}
			}
		}
	}

	private ManualConfig decompress(byte[] data) throws IOException {
		Inflater inf = new Inflater(true);
		try {
			InflaterInputStream gin = new InflaterInputStream(new ByteArrayInputStream(data), inf);
			ByteArrayOutputStream bout = new ByteArrayOutputStream();
			IO.INSTANCE.copy(gin, bout);
			return JSON.parseObject(bout.toByteArray(), ManualConfig.class);
		} finally {
			if (inf != null) {
				inf.end();
			}
		}
	}

	@Override
	public void cancel(FetchManualConfigCommandV6 cmd) {
		m_lock.lock();
		try {
			m_cmds.remove(cmd.getHeader().getCorrelationId());
		} finally {
			m_lock.unlock();
		}
	}

}
