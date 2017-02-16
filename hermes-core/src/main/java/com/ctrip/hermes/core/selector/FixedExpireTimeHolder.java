/**
 * 
 */
package com.ctrip.hermes.core.selector;

/**
 * @author marsqing
 *
 *         Jun 28, 2016 2:28:30 PM
 */
public class FixedExpireTimeHolder implements ExpireTimeHolder {

	private long m_expireTime;

	public FixedExpireTimeHolder(long expireTime) {
		m_expireTime = expireTime;
	}

	@Override
	public long currentExpireTime() {
		return m_expireTime;
	}

}
