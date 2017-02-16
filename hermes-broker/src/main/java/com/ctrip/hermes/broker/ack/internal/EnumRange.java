package com.ctrip.hermes.broker.ack.internal;

import java.util.ArrayList;
import java.util.List;

import org.unidal.tuple.Pair;

public class EnumRange<T> {

	private List<Pair<Long, T>> m_offsets;

	public EnumRange() {
		m_offsets = new ArrayList<>();
	}

	public EnumRange(List<Pair<Long, T>> offsets) {
		m_offsets = offsets;
	}

	public void addOffset(Long offset, T ctx) {
		m_offsets.add(new Pair<>(offset, ctx));
	}

	public void addOffset(Pair<Long, T> offset) {
		m_offsets.add(offset);
	}

	public List<Pair<Long, T>> getOffsets() {
		return m_offsets;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((m_offsets == null) ? 0 : m_offsets.hashCode());
		return result;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		EnumRange other = (EnumRange) obj;
		if (m_offsets == null) {
			if (other.m_offsets != null)
				return false;
		} else if (!m_offsets.equals(other.m_offsets))
			return false;
		return true;
	}

}
