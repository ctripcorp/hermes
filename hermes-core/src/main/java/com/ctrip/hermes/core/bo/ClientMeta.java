package com.ctrip.hermes.core.bo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.unidal.helper.Files.AutoClose;
import org.unidal.helper.Files.IO;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Idc;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Property;
import com.ctrip.hermes.meta.entity.Storage;

public class ClientMeta {

	public static byte[] serialize(Meta meta) {
		ClientMeta clientMeta = convertToClientMeta(meta);
		return clientMeta.compress();
	}

	public static Meta deserialize(byte[] bytes) throws Exception {
		ClientMeta clientMeta = decompress(bytes);
		return clientMeta.convertToMeta();
	}

	private static ClientMeta convertToClientMeta(Meta meta) {
		ClientMeta clientMeta = new ClientMeta();
		clientMeta.m_version = meta.getVersion();
		clientMeta.m_id = meta.getId();
		clientMeta.m_idcs = meta.getIdcs();
		clientMeta.m_endpoints = meta.getEndpoints();

		for (String endpoint : meta.getEndpoints().keySet()) {
			clientMeta.m_endpointMapping.put(endpoint, new LinkedHashMap<Long, List<Integer>>());
		}

		for (com.ctrip.hermes.meta.entity.Topic topic : meta.getTopics().values()) {
			for (Partition partition : topic.getPartitions()) {
				if (partition.getEndpoint() != null) {
					Map<Long, List<Integer>> tpMap = clientMeta.m_endpointMapping.get(partition.getEndpoint());
					List<Integer> pList = tpMap.get(topic.getId());
					if (pList == null) {
						pList = new ArrayList<>();
						tpMap.put(topic.getId(), pList);
					}
					pList.add(partition.getId());
				}
			}
		}

		for (com.ctrip.hermes.meta.entity.Topic topic : meta.getTopics().values()) {
			Topic clientTopic = new Topic();
			clientTopic.setCodecType(topic.getCodecType());
			clientTopic.setConsumerRetryPolicy(topic.getConsumerRetryPolicy());
			clientTopic.setEndpointType(topic.getEndpointType());
			clientTopic.setId(topic.getId());
			clientTopic.setIdcPolicy(topic.getIdcPolicy());
			clientTopic.setName(topic.getName());
			clientTopic.setPriorityMessageEnabled(topic.isPriorityMessageEnabled());
			clientTopic.setStorageType(topic.getStorageType());
			for (com.ctrip.hermes.meta.entity.ConsumerGroup consumerGroup : topic.getConsumerGroups()) {
				ConsumerGroup clientConsumerGroup = new ConsumerGroup();
				clientConsumerGroup.setName(consumerGroup.getName());
				clientConsumerGroup.setId(consumerGroup.getId());
				clientConsumerGroup.setIdcPolicy(consumerGroup.getIdcPolicy());
				clientConsumerGroup.setRetryPolicy(consumerGroup.getRetryPolicy());
				clientTopic.addConsumerGroup(clientConsumerGroup);
			}
			clientMeta.m_topics.put(clientTopic.getId(), clientTopic);
		}

		for (Storage storage : meta.getStorages().values()) {
			Storage clientStorage = new Storage(storage.getType());
			if (!Storage.MYSQL.equals(storage.getType())) {
				for (Datasource datasource : storage.getDatasources()) {
					Datasource clientDatasource = new Datasource();
					clientDatasource.setId(datasource.getId());
					for (Property property : datasource.getProperties().values()) {
						Property clientProperty = new Property(property.getName());
						clientProperty.setValue(property.getValue());
						clientDatasource.addProperty(clientProperty);
					}
					clientStorage.addDatasource(clientDatasource);
				}
			}
			clientMeta.m_storages.put(clientStorage.getType(), clientStorage);
		}

		return clientMeta;
	}

	private static ClientMeta decompress(byte[] compressedJsonBytes) throws Exception {
		GZIPInputStream gzipInputStream = null;
		ClientMeta clientMeta = null;
		try {
			gzipInputStream = new GZIPInputStream(new ByteArrayInputStream(compressedJsonBytes));
			clientMeta = JSON.parseObject(gzipInputStream, ClientMeta.class);
			return clientMeta;
		} finally {
			if (gzipInputStream != null) {
				gzipInputStream.close();
			}
		}
	}

	public byte[] compress() {
		ByteArrayInputStream input = new ByteArrayInputStream((JSON.toJSONBytes(this)));
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		try {
			GZIPOutputStream gout = new GZIPOutputStream(bout);
			IO.INSTANCE.copy(input, gout, AutoClose.INPUT_OUTPUT);
		} catch (IOException e) {
			throw new RuntimeException("Unexpected exception when encode clientMeta", e);
		}

		return bout.toByteArray();
	}

	private Meta convertToMeta() {
		Meta meta = new Meta();
		meta.setVersion(m_version);
		meta.setId(m_id);

		for (Idc idc : m_idcs.values()) {
			meta.addIdc(idc);
		}

		for (Endpoint endpoint : m_endpoints.values()) {
			meta.addEndpoint(endpoint);
		}

		for (Storage storage : m_storages.values()) {
			meta.addStorage(storage);
		}

		for (Entry<String, Map<Long, List<Integer>>> endpointMapping : m_endpointMapping.entrySet()) {
			String endpoint = endpointMapping.getKey();
			for (Entry<Long, List<Integer>> tp : endpointMapping.getValue().entrySet()) {
				Topic topic = m_topics.get(tp.getKey());
				for (Integer p : tp.getValue()) {
					Partition partition = new Partition(p).setEndpoint(endpoint);
					if (Storage.KAFKA.equals(topic.getStorageType())) {
						partition.setReadDatasource("kafka-consumer");
						partition.setWriteDatasource("kafka-producer");
					}
					topic.addPartition(partition);
				}
			}
		}

		for (Topic topic : m_topics.values()) {
			com.ctrip.hermes.meta.entity.Topic t = new com.ctrip.hermes.meta.entity.Topic();
			t.setCodecType(topic.getCodecType());
			t.setConsumerRetryPolicy(topic.getConsumerRetryPolicy());
			t.setEndpointType(topic.getEndpointType());
			t.setId(topic.getId());
			t.setIdcPolicy(topic.getIdcPolicy());
			t.setName(topic.getName());
			t.setPriorityMessageEnabled(topic.isPriorityMessageEnabled());
			t.setStorageType(topic.getStorageType());

			for (Partition partition : topic.getPartitions()) {
				t.addPartition(partition);
			}

			for (ConsumerGroup consumerGroup : topic.getConsumerGroups()) {
				com.ctrip.hermes.meta.entity.ConsumerGroup c = new com.ctrip.hermes.meta.entity.ConsumerGroup();
				c.setName(consumerGroup.getName());
				c.setId(consumerGroup.getId());
				c.setIdcPolicy(consumerGroup.getIdcPolicy());
				c.setRetryPolicy(consumerGroup.getRetryPolicy());
				t.addConsumerGroup(c);
			}
			meta.addTopic(t);
		}

		return meta;
	}

	private Long m_version;

	private Long m_id;

	private Map<String, Idc> m_idcs = new LinkedHashMap<String, Idc>();// kafka topic cares primary idc;

	private Map<String, Endpoint> m_endpoints = new LinkedHashMap<String, Endpoint>();

	private Map<String, Map<Long, List<Integer>>> m_endpointMapping = new LinkedHashMap<String, Map<Long, List<Integer>>>();

	private Map<Long, Topic> m_topics = new LinkedHashMap<>();// remove partitions attribute, use partition count

	private Map<String, Storage> m_storages = new LinkedHashMap<String, Storage>();// all storage needed, remove mysql datasources,
	                                                                               // broker use mysql storage

	public Long getVersion() {
		return m_version;
	}

	public void setVersion(Long version) {
		m_version = version;
	}

	public Long getId() {
		return m_id;
	}

	public void setId(Long id) {
		m_id = id;
	}

	public Map<String, Idc> getIdcs() {
		return m_idcs;
	}

	public void setIdcs(Map<String, Idc> idcs) {
		m_idcs = idcs;
	}

	public Map<String, Endpoint> getEndpoints() {
		return m_endpoints;
	}

	public void setEndpoints(Map<String, Endpoint> endpoints) {
		m_endpoints = endpoints;
	}

	public Map<String, Map<Long, List<Integer>>> getEndpointMapping() {
		return m_endpointMapping;
	}

	public void setEndpointMapping(Map<String, Map<Long, List<Integer>>> endpointMapping) {
		m_endpointMapping = endpointMapping;
	}

	public Map<Long, Topic> getTopics() {
		return m_topics;
	}

	public void setTopics(Map<Long, Topic> topics) {
		m_topics = topics;
	}

	public Map<String, Storage> getStorages() {
		return m_storages;
	}

	public void setStorages(Map<String, Storage> storages) {
		m_storages = storages;
	}

	public static class Topic {

		@Override
		public String toString() {
			return "Topic [name=" + name + ", codecType=" + codecType + ", consumerRetryPolicy=" + consumerRetryPolicy
			      + ", endpointType=" + endpointType + ", id=" + id + ", idcPolicy=" + idcPolicy
			      + ", priorityMessageEnabled=" + priorityMessageEnabled + ", storageType=" + storageType
			      + ", consumerGroups=" + consumerGroups + ", partitions=" + partitions + "]";
		}

		@JSONField(name = "n")
		private String name;

		@JSONField(name = "ct")
		private String codecType;

		@JSONField(name = "cr")
		private String consumerRetryPolicy = "3:[3,3000]";

		@JSONField(name = "e")
		private String endpointType = "broker";

		@JSONField(name = "i")
		private Long id;

		@JSONField(name = "ip")
		private String idcPolicy = "primary";

		@JSONField(name = "p")
		private Boolean priorityMessageEnabled = true;

		@JSONField(name = "s")
		private String storageType;

		@JSONField(name = "c")
		private List<ConsumerGroup> consumerGroups = new ArrayList<ConsumerGroup>();

		private transient List<Partition> partitions = new ArrayList<Partition>();

		public Topic addConsumerGroup(ConsumerGroup consumerGroup) {
			consumerGroups.add(consumerGroup);
			return this;
		}

		public Topic addPartition(Partition partition) {
			partitions.add(partition);
			return this;
		}

		public String getCodecType() {
			return codecType;
		}

		public List<ConsumerGroup> getConsumerGroups() {
			return consumerGroups;
		}

		public String getConsumerRetryPolicy() {
			return consumerRetryPolicy;
		}

		public String getEndpointType() {
			return endpointType;
		}

		public Long getId() {
			return id;
		}

		public String getIdcPolicy() {
			return idcPolicy;
		}

		public String getName() {
			return name;
		}

		public List<Partition> getPartitions() {
			return partitions;
		}

		public Boolean getPriorityMessageEnabled() {
			return priorityMessageEnabled;
		}

		public String getStorageType() {
			return storageType;
		}

		public boolean isPriorityMessageEnabled() {
			return priorityMessageEnabled != null && priorityMessageEnabled.booleanValue();
		}

		public void setCodecType(String codecType) {
			this.codecType = codecType;
		}

		public void setConsumerGroups(List<ConsumerGroup> consumerGroups) {
			this.consumerGroups = consumerGroups;
		}

		public void setConsumerRetryPolicy(String consumerRetryPolicy) {
			this.consumerRetryPolicy = consumerRetryPolicy;
		}

		public void setEndpointType(String endpointType) {
			this.endpointType = endpointType;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public void setIdcPolicy(String idcPolicy) {
			this.idcPolicy = idcPolicy;
		}

		public void setName(String name) {
			this.name = name;
		}

		public void setPartitions(List<Partition> partitions) {
			this.partitions = partitions;
		}

		public void setPriorityMessageEnabled(Boolean priorityMessageEnabled) {
			this.priorityMessageEnabled = priorityMessageEnabled;
		}

		public void setStorageType(String storageType) {
			this.storageType = storageType;
		}

	}

	public static class ConsumerGroup {

		@Override
		public String toString() {
			return "ConsumerGroup [id=" + id + ", name=" + name + ", retryPolicy=" + retryPolicy + ", idcPolicy="
			      + idcPolicy + "]";
		}

		@JSONField(name = "i")
		private Integer id;

		@JSONField(name = "n")
		private String name;

		@JSONField(name = "r")
		private String retryPolicy;

		@JSONField(name = "ip")
		private String idcPolicy = "primary";

		public Integer getId() {
			return id;
		}

		public String getIdcPolicy() {
			return idcPolicy;
		}

		public String getName() {
			return name;
		}

		public String getRetryPolicy() {
			return retryPolicy;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public void setIdcPolicy(String idcPolicy) {
			this.idcPolicy = idcPolicy;
		}

		public void setName(String name) {
			this.name = name;
		}

		public void setRetryPolicy(String retryPolicy) {
			this.retryPolicy = retryPolicy;
		}

	}

}
