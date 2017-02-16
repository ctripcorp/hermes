package com.ctrip.hermes.core.bo;

/**
 * Topic-Partition-Priority Wrapper
 * 
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class Tpp {
	private String m_topic;

	private int m_partition;

	private boolean m_priority;

	public Tpp(String topic, int partition, boolean priority) {
		m_topic = topic;
		m_partition = partition;
		m_priority = priority;
	}

	public void setTopic(String topic) {
		m_topic = topic;
	}

	public void setPartition(int partition) {
		m_partition = partition;
	}

	public void setPriority(boolean priority) {
		m_priority = priority;
	}

	public String getTopic() {
		return m_topic;
	}

	public int getPartition() {
		return m_partition;
	}

	public boolean isPriority() {
		return m_priority;
	}

	public int getPriorityInt() {
		// TODO move to other place
		return isPriority() ? 0 : 1;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + m_partition;
		result = prime * result + (m_priority ? 1231 : 1237);
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
		Tpp other = (Tpp) obj;
		if (m_partition != other.m_partition)
			return false;
		if (m_priority != other.m_priority)
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
		return "Tpp [m_topic=" + m_topic + ", m_partition=" + m_partition + ", m_priority=" + m_priority + "]";
	}

}
