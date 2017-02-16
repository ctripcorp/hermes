package com.ctrip.hermes.consumer.engine.ack;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class AckHolderScanningResult<T> {
	private List<T> m_acked = new LinkedList<>();

	private List<T> m_nacked = new LinkedList<>();

	public void addAcked(T item) {
		m_acked.add(item);
	}

	public void addNacked(T item) {
		m_nacked.add(item);
	}

	public List<T> getAcked() {
		return m_acked;
	}

	public List<T> getNacked() {
		return m_nacked;
	}

}
