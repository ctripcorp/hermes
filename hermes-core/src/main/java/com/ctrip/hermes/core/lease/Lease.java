package com.ctrip.hermes.core.lease;

import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.fastjson.annotation.JSONField;
import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class Lease {
	private long m_id;

	private AtomicLong m_expireTime = new AtomicLong(0);

	public Lease() {
	}

	public Lease(long id, long expireTime) {
		m_expireTime = new AtomicLong(expireTime);
		m_id = id;
	}

	public void setId(long id) {
		m_id = id;
	}

	public long getId() {
		return m_id;
	}

	public void setExpireTime(long expireTime) {
		m_expireTime.set(expireTime);
	}

	public long getExpireTime() {
		return m_expireTime.get();
	}

	@JsonIgnore
	@JSONField(serialize = false)
	public boolean isExpired() {
		return getRemainingTime() <= 0;
	}

	@JsonIgnore
	@JSONField(serialize = false)
	public long getRemainingTime() {
		return m_expireTime.get() - System.currentTimeMillis();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (m_id ^ (m_id >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Lease other = (Lease) obj;
		if (m_id != other.m_id)
			return false;
		return true;
	}

}
