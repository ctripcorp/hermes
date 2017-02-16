package com.ctrip.hermes.core.bo;

/**
 * Topic-Partition-GroupId Wrapper
 * 
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class Tpg {
	private String m_topic;

	private int m_partition;

	private String m_groupId;

	public Tpg() {
	}

	public Tpg(String topic, int partition, String groupId) {
		m_topic = topic;
		m_partition = partition;
		m_groupId = groupId;
	}

	public String getTopic() {
		return m_topic;
	}

	public int getPartition() {
		return m_partition;
	}

	public String getGroupId() {
		return m_groupId;
	}

	public void setTopic(String topic) {
		m_topic = topic;
	}

	public void setPartition(int partition) {
		m_partition = partition;
	}

	public void setGroupId(String groupId) {
		m_groupId = groupId;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((m_groupId == null) ? 0 : m_groupId.hashCode());
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
		Tpg other = (Tpg) obj;
		if (m_groupId == null) {
			if (other.m_groupId != null)
				return false;
		} else if (!m_groupId.equals(other.m_groupId))
			return false;
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
		return "Tpg [m_topic=" + m_topic + ", m_partition=" + m_partition + ", m_groupId=" + m_groupId + "]";
	}

}
