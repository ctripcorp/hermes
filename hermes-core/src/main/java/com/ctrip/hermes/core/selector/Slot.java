/**
 * 
 */
package com.ctrip.hermes.core.selector;

/**
 * @author marsqing
 *
 *         Jun 20, 2016 11:51:28 PM
 */
public class Slot {

	private int index;
	private long offset;
	private long minFireInterval;

	public Slot(int index, long offset) {
		this.index = index;
		this.offset = offset;
	}

	public Slot(int index, long offset, long minFireInterval) {
		this.index = index;
		this.offset = offset;
		this.minFireInterval = minFireInterval;
	}

	public int getIndex() {
		return index;
	}

	public long getOffset() {
		return offset;
	}

	public long getMinFireInterval() {
		return minFireInterval;
	}

	@Override
	public String toString() {
		return "Slot [index=" + index + ", offset=" + offset + ", minFireInterval=" + minFireInterval + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + index;
		result = prime * result + (int) (minFireInterval ^ (minFireInterval >>> 32));
		result = prime * result + (int) (offset ^ (offset >>> 32));
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
		Slot other = (Slot) obj;
		if (index != other.index)
			return false;
		if (minFireInterval != other.minFireInterval)
			return false;
		if (offset != other.offset)
			return false;
		return true;
	}

}
