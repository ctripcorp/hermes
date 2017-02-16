package com.ctrip.hermes.metaserver.commons;

import java.util.List;
import java.util.Map;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.bo.HostPort;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Idc;
import com.ctrip.hermes.meta.entity.Server;
import com.ctrip.hermes.metaserver.cluster.Role;

public class MetaServerStatusResponse {

	private String currentHost;

	private Role role;

	private HostPort leaderInfo;

	private Map<String, Assignment<Integer>> brokerAssignments;

	private Map<Pair<String, Integer>, Map<String, ClientLeaseInfo>> brokerLeases;

	private Map<Tpg, Map<String, ClientLeaseInfo>> consumerLeases;

	private Assignment<String> metaServerAssignments;

	private Map<Pair<String, String>, Assignment<Integer>> consumerAssignments;

	private Map<Pair<String, Integer>, Endpoint> configedBrokers;

	private Map<String, Map<String, ClientContext>> m_effectiveBrokers;

	private Map<String, ClientContext> m_runningBrokers;

	private List<Server> m_configedMetaServers;

	private Map<String, Idc> m_idcs;

	public String getCurrentHost() {
		return currentHost;
	}

	public void setCurrentHost(String currentHost) {
		this.currentHost = currentHost;
	}

	public Role getRole() {
		return role;
	}

	public void setRole(Role role) {
		this.role = role;
	}

	public Map<String, Assignment<Integer>> getBrokerAssignments() {
		return brokerAssignments;
	}

	public void setBrokerAssignments(Map<String, Assignment<Integer>> brokerAssignments) {
		this.brokerAssignments = brokerAssignments;
	}

	public Map<Pair<String, Integer>, Map<String, ClientLeaseInfo>> getBrokerLeases() {
		return brokerLeases;
	}

	public void setBrokerLeases(Map<Pair<String, Integer>, Map<String, ClientLeaseInfo>> brokerLeases) {
		this.brokerLeases = brokerLeases;
	}

	public Map<Tpg, Map<String, ClientLeaseInfo>> getConsumerLeases() {
		return consumerLeases;
	}

	public void setConsumerLeases(Map<Tpg, Map<String, ClientLeaseInfo>> consumerLeases) {
		this.consumerLeases = consumerLeases;
	}

	public HostPort getLeaderInfo() {
		return leaderInfo;
	}

	public void setLeaderInfo(HostPort leaderInfo) {
		this.leaderInfo = leaderInfo;
	}

	public Assignment<String> getMetaServerAssignments() {
		return metaServerAssignments;
	}

	public void setMetaServerAssignments(Assignment<String> metaServerAssignments) {
		this.metaServerAssignments = metaServerAssignments;
	}

	public void setConsumerAssignments(Map<Pair<String, String>, Assignment<Integer>> consumerAssignments) {
		this.consumerAssignments = consumerAssignments;
	}

	public Map<Pair<String, String>, Assignment<Integer>> getConsumerAssignments() {
		return consumerAssignments;
	}

	public void setConfigedBrokers(Map<Pair<String, Integer>, Endpoint> configedBrokers) {
		this.configedBrokers = configedBrokers;
	}

	public Map<Pair<String, Integer>, Endpoint> getConfigedBrokers() {
		return configedBrokers;
	}

	public Map<String, Map<String, ClientContext>> getEffectiveBrokers() {
		return m_effectiveBrokers;
	}

	public void setEffectiveBrokers(Map<String, Map<String, ClientContext>> effectiveBrokers) {
		m_effectiveBrokers = effectiveBrokers;
	}

	public void setRunningBrokers(Map<String, ClientContext> runningBrokers) {
		m_runningBrokers = runningBrokers;
	}

	public Map<String, ClientContext> getRunningBrokers() {
		return m_runningBrokers;
	}

	public void setConfigedMetaServers(List<Server> configedMetaServers) {
		m_configedMetaServers = configedMetaServers;
	}

	public List<Server> getConfigedMetaServers() {
		return m_configedMetaServers;
	}

	public void setIdcs(Map<String, Idc> idcs) {
		m_idcs = idcs;
	}

	public Map<String, Idc> getIdcs() {
		return m_idcs;
	}

}
