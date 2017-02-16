package com.ctrip.hermes.metaserver.event;

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Named;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Named(type = Guard.class)
public class Guard {

	private static final Logger log = LoggerFactory.getLogger(Guard.class);

	private AtomicLong m_version = new AtomicLong(0);

	public long upgradeVersion() {
		long version = m_version.incrementAndGet();
		log.info("Guard version upgrade to {}", version);
		return version;
	}

	public boolean pass(long version) {
		return version == m_version.get();
	}
}
