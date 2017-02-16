package com.ctrip.hermes.metaserver.rest.resource;

import java.util.List;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.ctrip.hermes.core.bo.HostPort;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.ZookeeperEnsemble;
import com.ctrip.hermes.metaserver.cluster.ClusterStateHolder;
import com.ctrip.hermes.metaserver.cluster.Role;
import com.ctrip.hermes.metaserver.config.MetaServerConfig;
import com.ctrip.hermes.metaserver.rest.commons.RestException;
import com.ctrip.hermes.metaservice.zk.ZKClient;

@Path("/management/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class ManagementResource {

	private ZKClient m_zkClient = PlexusComponentLocator.lookup(ZKClient.class);

	private ClusterStateHolder m_clusterStateHolder = PlexusComponentLocator.lookup(ClusterStateHolder.class);

	@POST
	@Path("zk/resume")
	public Response resumeZK() {
		m_zkClient.resume();
		return Response.status(Status.OK).entity(Boolean.TRUE).build();
	}

	@POST
	@Path("zk/pauseAndSwitch")
	public Response pauseAndwitchZK() {
		try {
			boolean switched = m_zkClient.pauseAndSwitchPrimaryEnsemble();
			return Response.status(Status.OK).entity(switched).build();
		} catch (Exception e) {
			throw new RestException("Switch failed!", e);
		}
	}

	@GET
	@Path("status")
	public ManagementStatus status() {
		ManagementStatus status = new ManagementStatus();
		status.setCurrentHost(PlexusComponentLocator.lookup(MetaServerConfig.class).getMetaServerName());
		status.setRole(PlexusComponentLocator.lookup(ClusterStateHolder.class).getRole());
		status.setLeaderInfo(PlexusComponentLocator.lookup(ClusterStateHolder.class).getLeader());
		status.setZookeeperEnsembles(PlexusComponentLocator.lookup(ZKClient.class).getZookeeperEnsembles());
		status.setZkConnected(PlexusComponentLocator.lookup(ClusterStateHolder.class).isConnected());
		status.setZkClientPaused(PlexusComponentLocator.lookup(ZKClient.class).isPaused());
		status.setLeaseAssigning(PlexusComponentLocator.lookup(ClusterStateHolder.class).isLeaseAssigning());
		return status;
	}

	@POST
	@Path("leaseAssigning/stop")
	public Response stopLeaseAssigning() {
		m_clusterStateHolder.setLeaseAssigning(false);
		return Response.ok().build();
	}

	@POST
	@Path("leaseAssigning/start")
	public Response startLeaseAssigning() {
		m_clusterStateHolder.setLeaseAssigning(true);
		return Response.ok().build();
	}

	public static class ManagementStatus {
		private String currentHost;

		private Role role;

		private HostPort leaderInfo;

		private boolean m_zkConnected;

		private boolean m_zkClientPaused;

		private List<ZookeeperEnsemble> m_zookeeperEnsembles;

		private boolean m_leaseAssigning;

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

		public HostPort getLeaderInfo() {
			return leaderInfo;
		}

		public void setLeaderInfo(HostPort leaderInfo) {
			this.leaderInfo = leaderInfo;
		}

		public boolean isZkConnected() {
			return m_zkConnected;
		}

		public void setZkConnected(boolean zkConnected) {
			m_zkConnected = zkConnected;
		}

		public boolean isZkClientPaused() {
			return m_zkClientPaused;
		}

		public void setZkClientPaused(boolean zkClientPaused) {
			m_zkClientPaused = zkClientPaused;
		}

		public List<ZookeeperEnsemble> getZookeeperEnsembles() {
			return m_zookeeperEnsembles;
		}

		public void setZookeeperEnsembles(List<ZookeeperEnsemble> zookeeperEnsembles) {
			m_zookeeperEnsembles = zookeeperEnsembles;
		}

		public boolean isLeaseAssigning() {
			return m_leaseAssigning;
		}

		public void setLeaseAssigning(boolean m_leaseAssigning) {
			this.m_leaseAssigning = m_leaseAssigning;
		}

	}
}
