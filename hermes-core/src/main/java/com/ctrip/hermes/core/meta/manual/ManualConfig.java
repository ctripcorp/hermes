package com.ctrip.hermes.core.meta.manual;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

import org.unidal.helper.Files.AutoClose;
import org.unidal.helper.Files.IO;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.meta.entity.Meta;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class ManualConfig {
	private long m_version;

	private Meta m_meta;

	private Set<ManualConsumerAssignemnt> m_consumerAssignments = new HashSet<>();

	private Set<ManualBrokerAssignemnt> m_brokerAssignments = new HashSet<>();

	private transient volatile byte[] m_bytes;

	public ManualConfig() {
		this(-1L, null, null, null);
	}

	public ManualConfig(long version, Meta meta, Set<ManualConsumerAssignemnt> consumerAssignments,
	      Set<ManualBrokerAssignemnt> brokerAssignments) {
		m_version = version;
		m_meta = meta;
		m_consumerAssignments = consumerAssignments;
		m_brokerAssignments = brokerAssignments;
	}

	public long getVersion() {
		return m_version;
	}

	public void setVersion(long version) {
		m_version = version;
	}

	public Meta getMeta() {
		return m_meta;
	}

	public void setMeta(Meta meta) {
		m_meta = meta;
	}

	public Set<ManualConsumerAssignemnt> getConsumerAssignments() {
		return m_consumerAssignments;
	}

	public void setConsumerAssignments(Set<ManualConsumerAssignemnt> consumerAssignments) {
		m_consumerAssignments = consumerAssignments;
	}

	public Set<ManualBrokerAssignemnt> getBrokerAssignments() {
		return m_brokerAssignments;
	}

	public void setBrokerAssignments(Set<ManualBrokerAssignemnt> brokerAssignments) {
		m_brokerAssignments = brokerAssignments;
	}

	public byte[] toBytes() {
		if (m_bytes == null) {
			synchronized (this) {
				if (m_bytes == null) {
					m_bytes = compress(JSON.toJSONBytes(this));
				}
			}
		}

		return m_bytes;
	}

	private byte[] compress(byte[] bytes) {
		ByteArrayInputStream input = new ByteArrayInputStream(bytes);
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		Deflater def = new Deflater(5, true);
		try {
			DeflaterOutputStream gout = new DeflaterOutputStream(bout, def);
			IO.INSTANCE.copy(input, gout, AutoClose.INPUT_OUTPUT);
			return bout.toByteArray();
		} catch (IOException e) {
			throw new RuntimeException("Exception occurred while compressing manual config.", e);
		} finally {
			if (def != null) {
				def.end();
			}
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (m_version ^ (m_version >>> 32));
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
		ManualConfig other = (ManualConfig) obj;
		if (m_version != other.m_version)
			return false;
		return true;
	}

	public static class ManualConsumerAssignemnt {
		private String m_topic;

		private String m_group;

		private Map<String, List<Integer>> m_assignment = new HashMap<>();

		public ManualConsumerAssignemnt() {
		}

		public ManualConsumerAssignemnt(String topic, String group, Map<String, List<Integer>> assignment) {
			m_topic = topic;
			m_group = group;
			m_assignment = assignment;
		}

		public String getTopic() {
			return m_topic;
		}

		public void setTopic(String topic) {
			m_topic = topic;
		}

		public String getGroup() {
			return m_group;
		}

		public void setGroup(String group) {
			m_group = group;
		}

		public Map<String, List<Integer>> getAssignment() {
			return m_assignment;
		}

		public void setAssignment(Map<String, List<Integer>> assignment) {
			m_assignment = assignment;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((m_group == null) ? 0 : m_group.hashCode());
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
			ManualConsumerAssignemnt other = (ManualConsumerAssignemnt) obj;
			if (m_group == null) {
				if (other.m_group != null)
					return false;
			} else if (!m_group.equals(other.m_group))
				return false;
			if (m_topic == null) {
				if (other.m_topic != null)
					return false;
			} else if (!m_topic.equals(other.m_topic))
				return false;
			return true;
		}

	}

	public static class ManualBrokerAssignemnt {
		private String m_brokerId;

		private Map<String, List<Integer>> m_ownedTopicPartitions = new HashMap<>();

		public ManualBrokerAssignemnt() {
		}

		public ManualBrokerAssignemnt(String brokerId, Map<String, List<Integer>> ownedTopicPartitions) {
			m_brokerId = brokerId;
			m_ownedTopicPartitions = ownedTopicPartitions;
		}

		public String getBrokerId() {
			return m_brokerId;
		}

		public void setBrokerId(String brokerId) {
			m_brokerId = brokerId;
		}

		public Map<String, List<Integer>> getOwnedTopicPartitions() {
			return m_ownedTopicPartitions;
		}

		public void setOwnedTopicPartitions(Map<String, List<Integer>> ownedTopicPartitions) {
			m_ownedTopicPartitions = ownedTopicPartitions;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((m_brokerId == null) ? 0 : m_brokerId.hashCode());
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
			ManualBrokerAssignemnt other = (ManualBrokerAssignemnt) obj;
			if (m_brokerId == null) {
				if (other.m_brokerId != null)
					return false;
			} else if (!m_brokerId.equals(other.m_brokerId))
				return false;
			return true;
		}

	}
}
