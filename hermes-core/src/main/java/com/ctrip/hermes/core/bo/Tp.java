/**
 * 
 */
package com.ctrip.hermes.core.bo;

/**
 * @author marsqing
 *
 *         Jun 22, 2016 3:53:28 PM
 */
public class Tp {

	private String m_topic;

	private int m_partition;

	public Tp() {
	}

	public Tp(String m_topic, int m_partition) {
		this.m_topic = m_topic;
		this.m_partition = m_partition;
	}

	public String getTopic() {
		return m_topic;
	}

	public int getPartition() {
		return m_partition;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + m_partition;
		result = prime * result + ((m_topic == null) ? 0 : m_topic.hashCode());
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
		Tp other = (Tp) obj;
		if (m_partition != other.m_partition)
			return false;
		if (m_topic == null) {
			if (other.m_topic != null)
				return false;
		} else if (!m_topic.equals(other.m_topic))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Tp [m_topic=" + m_topic + ", m_partition=" + m_partition + "]";
	}

}
