/**
 * 
 */
package com.ctrip.hermes.core.selector;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author marsqing
 *
 *         Jul 4, 2016 5:55:15 PM
 */
public class DefaultOffsetGenerator implements OffsetGenerator {

	private AtomicLong m_seed = new AtomicLong(1);

	@Override
	public long nextOffset(int delta) {
		return m_seed.addAndGet(delta);
	}

}
