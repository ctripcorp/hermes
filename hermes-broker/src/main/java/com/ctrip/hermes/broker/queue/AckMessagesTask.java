package com.ctrip.hermes.broker.queue;

import io.netty.channel.Channel;

import java.util.ArrayList;
import java.util.List;

import com.ctrip.hermes.core.bo.AckContext;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class AckMessagesTask {

	private long m_correlationId;

	private String m_topic;

	private int m_partition;

	private String m_groupId;

	private Channel m_channel;

	private long m_expireTime;

	private int m_cmdVersion;

	private List<AckContext> m_ackedContexts = new ArrayList<>();

	private List<AckContext> m_ackedPriorityContexts = new ArrayList<>();

	private List<AckContext> m_ackedResendContexts = new ArrayList<>();

	private List<AckContext> m_nackedContexts = new ArrayList<>();

	private List<AckContext> m_nackedPriorityContexts = new ArrayList<>();

	private List<AckContext> m_nackedResendContexts = new ArrayList<>();

	public AckMessagesTask(String topic, int partition, int cmdVersion, String groupId, long correlationId,
	      Channel channel, long expireTime) {
		m_topic = topic;
		m_partition = partition;
		m_cmdVersion = cmdVersion;
		m_groupId = groupId;
		m_channel = channel;
		m_correlationId = correlationId;
		m_expireTime = expireTime;
	}

	public int getCmdVersion() {
		return m_cmdVersion;
	}

	public void setCmdVersion(int cmdVersion) {
		m_cmdVersion = cmdVersion;
	}

	public long getExpireTime() {
		return m_expireTime;
	}

	public void setExpireTime(long expireTime) {
		m_expireTime = expireTime;
	}

	public long getCorrelationId() {
		return m_correlationId;
	}

	public void setCorrelationId(long correlationId) {
		m_correlationId = correlationId;
	}

	public String getTopic() {
		return m_topic;
	}

	public void setTopic(String topic) {
		m_topic = topic;
	}

	public int getPartition() {
		return m_partition;
	}

	public void setPartition(int partition) {
		m_partition = partition;
	}

	public String getGroupId() {
		return m_groupId;
	}

	public void setGroupId(String groupId) {
		m_groupId = groupId;
	}

	public Channel getChannel() {
		return m_channel;
	}

	public void setChannel(Channel channel) {
		m_channel = channel;
	}

	public List<AckContext> getAckedContexts() {
		return m_ackedContexts;
	}

	public void setAckedContexts(List<AckContext> ackedContexts) {
		m_ackedContexts = ackedContexts;
	}

	public List<AckContext> getAckedPriorityContexts() {
		return m_ackedPriorityContexts;
	}

	public void setAckedPriorityContexts(List<AckContext> ackedPriorityContexts) {
		m_ackedPriorityContexts = ackedPriorityContexts;
	}

	public List<AckContext> getAckedResendContexts() {
		return m_ackedResendContexts;
	}

	public void setAckedResendContexts(List<AckContext> ackedResendContexts) {
		m_ackedResendContexts = ackedResendContexts;
	}

	public List<AckContext> getNackedContexts() {
		return m_nackedContexts;
	}

	public void setNackedContexts(List<AckContext> nackedContexts) {
		m_nackedContexts = nackedContexts;
	}

	public List<AckContext> getNackedPriorityContexts() {
		return m_nackedPriorityContexts;
	}

	public void setNackedPriorityContexts(List<AckContext> nackedPriorityContexts) {
		m_nackedPriorityContexts = nackedPriorityContexts;
	}

	public List<AckContext> getNackedResendContexts() {
		return m_nackedResendContexts;
	}

	public void setNackedResendContexts(List<AckContext> nackedResendContexts) {
		m_nackedResendContexts = nackedResendContexts;
	}

}
