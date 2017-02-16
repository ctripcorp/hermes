/**
 * 
 */
package com.ctrip.hermes.core.selector;

/**
 * @author marsqing
 *
 *         Jun 29, 2016 10:39:37 AM
 */
public class SlotMatchResult {

	private int index;
	private boolean match;
	private long awaitingOffset;
	private long triggeringOffset;

	public SlotMatchResult(int index, boolean match, long awaitingOffset, long triggeringOffset) {
		this.index = index;
		this.match = match;
		this.awaitingOffset = awaitingOffset;
		this.triggeringOffset = triggeringOffset;
	}

	public int getIndex() {
		return index;
	}

	public boolean isMatch() {
		return match;
	}

	public long getAwaitingOffset() {
		return awaitingOffset;
	}

	public long getTriggeringOffset() {
		return triggeringOffset;
	}

	public long biggerOffset() {
		return Math.max(awaitingOffset, triggeringOffset);
	}

	@Override
	public String toString() {
		return "SlotMatchResult [index=" + index + ", match=" + match + ", awaitingOffset=" + awaitingOffset + ", triggeringOffset=" + triggeringOffset + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (awaitingOffset ^ (awaitingOffset >>> 32));
		result = prime * result + index;
		result = prime * result + (match ? 1231 : 1237);
		result = prime * result + (int) (triggeringOffset ^ (triggeringOffset >>> 32));
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
		SlotMatchResult other = (SlotMatchResult) obj;
		if (awaitingOffset != other.awaitingOffset)
			return false;
		if (index != other.index)
			return false;
		if (match != other.match)
			return false;
		if (triggeringOffset != other.triggeringOffset)
			return false;
		return true;
	}

}
