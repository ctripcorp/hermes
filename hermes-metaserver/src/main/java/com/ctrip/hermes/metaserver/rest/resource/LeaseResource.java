package com.ctrip.hermes.metaserver.rest.resource;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.apache.http.HttpStatus;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.helper.Files.IO;

import sun.net.www.protocol.http.HttpURLConnection;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.bo.HostPort;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.constants.CatConstants;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.metaserver.broker.BrokerLeaseAllocator;
import com.ctrip.hermes.metaserver.broker.BrokerLeaseHolder;
import com.ctrip.hermes.metaserver.cluster.ClusterStateHolder;
import com.ctrip.hermes.metaserver.cluster.Role;
import com.ctrip.hermes.metaserver.commons.ClientContext;
import com.ctrip.hermes.metaserver.config.MetaServerConfig;
import com.ctrip.hermes.metaserver.consumer.ConsumerLeaseAllocator;
import com.ctrip.hermes.metaserver.consumer.ConsumerLeaseAllocatorLocator;
import com.ctrip.hermes.metaserver.consumer.ConsumerLeaseHolder;
import com.ctrip.hermes.metaserver.log.LoggerConstants;
import com.ctrip.hermes.metaserver.meta.MetaServerAssignmentHolder;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Message;
import com.dianping.cat.message.Transaction;

/**
 * 
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Path("/lease/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class LeaseResource {

	private static final String HEADER_PROXY_KEY = "hmProxy";

	private static final String HEADER_PROXY_VALUE = "true";

	private static final Logger log = LoggerFactory.getLogger(LeaseResource.class);

	private static final Logger traceLog = LoggerFactory.getLogger(LoggerConstants.TRACE);

	private static final long NO_STRATEGY_DELAY_TIME_MILLIS = 20 * 1000L;

	private static final long NO_ASSIGNMENT_DELAY_TIME_MILLIS = 5 * 1000L;

	private static final long PROXY_PASS_FAIL_DELAY_TIME_MILLIS = 5 * 1000L;

	private static final long EXCEPTION_CAUGHT_DELAY_TIME_MILLIS = 5 * 1000L;

	private static final long CLUSTER_NOT_READY_DELAY_TIME_MILLIS = 5 * 1000L;

	private static final long CLUSTER_NO_LEASE_ASSIGNING_DELAY_TIME_MILLIS = 5 * 1000L;

	private ConsumerLeaseAllocatorLocator m_consumerLeaseAllocatorLocator;

	private BrokerLeaseAllocator m_brokerLeaseAllocator;

	private SystemClockService m_systemClockService;

	private MetaServerAssignmentHolder m_metaServerAssignmentHolder;

	private ClusterStateHolder m_clusterStateHolder;

	private MetaServerConfig m_config;

	private BrokerLeaseHolder m_brokerLeaseHolder;

	private ConsumerLeaseHolder m_consumerLeaseHolder;

	public LeaseResource() {
		m_consumerLeaseAllocatorLocator = PlexusComponentLocator.lookup(ConsumerLeaseAllocatorLocator.class);
		m_brokerLeaseAllocator = PlexusComponentLocator.lookup(BrokerLeaseAllocator.class);
		m_systemClockService = PlexusComponentLocator.lookup(SystemClockService.class);
		m_metaServerAssignmentHolder = PlexusComponentLocator.lookup(MetaServerAssignmentHolder.class);
		m_config = PlexusComponentLocator.lookup(MetaServerConfig.class);
		m_clusterStateHolder = PlexusComponentLocator.lookup(ClusterStateHolder.class);
		m_brokerLeaseHolder = PlexusComponentLocator.lookup(BrokerLeaseHolder.class);
		m_consumerLeaseHolder = PlexusComponentLocator.lookup(ConsumerLeaseHolder.class);
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Path("consumer/acquire")
	public LeaseAcquireResponse tryAcquireConsumerLease(//
	      Tpg tpg, //
	      @QueryParam("sessionId") String sessionId,//
	      @QueryParam("host") @DefaultValue("-") String host,//
	      @QueryParam("currentTimestamp") @DefaultValue("-1") long clientCurrentTimestamp,//
	      @Context HttpServletRequest req) {

		long timeDiff = getClientTimeDiff(clientCurrentTimestamp);

		LeaseAcquireResponse response = null;
		Map<String, String> params = new HashMap<>();
		params.put("sessionId", sessionId);
		params.put("host", getRemoteAddr(host, req));

		Transaction transaction = Cat.newTransaction(CatConstants.TYPE_LEASE_ACQUIRE_CONSUMER,
		      tpg.getTopic() + ":" + tpg.getPartition() + ":" + tpg.getGroupId());
		transaction.addData("host", params.get("host"));
		transaction.addData("timeDiff", timeDiff);

		try {
			if (!m_clusterStateHolder.isConnected() || !m_consumerLeaseHolder.inited()) {
				response = new LeaseAcquireResponse(false, null, m_systemClockService.now()
				      + CLUSTER_NOT_READY_DELAY_TIME_MILLIS);
			} else {
				try {
					LeaseAcquireResponse leaseAcquireResponse = null;
					Transaction proxyTx = Cat.newTransaction(CatConstants.TYPE_LEASE_PROXY, "ConsumerAcquire");
					try {
						leaseAcquireResponse = proxyConsumerLeaseRequestIfNecessary(req, tpg.getTopic(), "/consumer/acquire",
						      params, tpg, proxyTx);
						proxyTx.setStatus(Transaction.SUCCESS);
					} catch (Exception e) {
						proxyTx.setStatus(e);
					} finally {
						proxyTx.complete();
					}

					if (leaseAcquireResponse == null) {
						if (m_clusterStateHolder.isLeaseAssigning()) {
							ConsumerLeaseAllocator leaseAllocator = m_consumerLeaseAllocatorLocator.findAllocator(
							      tpg.getTopic(), tpg.getGroupId());
							if (leaseAllocator != null) {
								response = leaseAllocator.tryAcquireLease(tpg, sessionId, getRemoteAddr(host, req));
							} else {
								response = new LeaseAcquireResponse(false, null, m_systemClockService.now()
								      + NO_STRATEGY_DELAY_TIME_MILLIS);
							}
						} else {
							response = new LeaseAcquireResponse(false, null, m_systemClockService.now()
							      + CLUSTER_NO_LEASE_ASSIGNING_DELAY_TIME_MILLIS);
						}
					} else {
						response = leaseAcquireResponse;
					}
				} catch (Exception e) {
					response = new LeaseAcquireResponse(false, null, m_systemClockService.now()
					      + EXCEPTION_CAUGHT_DELAY_TIME_MILLIS);
					transaction.setStatus(e);
				}
			}
			transaction.setStatus(Transaction.SUCCESS);
		} finally {
			if (traceLog.isInfoEnabled()) {
				traceLog.info("[ACCESS]acquire consumer lease. req:{}, tpg:{} resp({})", JSON.toJSONString(params),
				      JSON.toJSONString(tpg), JSON.toJSONString(response));
			}
			if (response.isAcquired()) {
				Cat.logEvent(CatConstants.TYPE_LEASE_ACQUIRED_CONSUMER, tpg.getTopic() + ":" + tpg.getPartition() + ":"
				      + tpg.getGroupId(), Message.SUCCESS, "host=" + params.get("host"));
			}

			transaction.complete();
		}
		return response.toClientLeaseResponse(timeDiff);
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Path("consumer/renew")
	public LeaseAcquireResponse tryRenewConsumerLease(//
	      Tpg tpg, //
	      @QueryParam("leaseId") long leaseId,//
	      @QueryParam("sessionId") String sessionId,//
	      @QueryParam("host") @DefaultValue("-") String host,//
	      @QueryParam("currentTimestamp") @DefaultValue("-1") long clientCurrentTimestamp,//
	      @Context HttpServletRequest req) {

		long timeDiff = getClientTimeDiff(clientCurrentTimestamp);

		LeaseAcquireResponse response = null;

		Map<String, String> params = new HashMap<>();
		params.put("sessionId", sessionId);
		params.put("leaseId", Long.toString(leaseId));
		params.put("host", getRemoteAddr(host, req));
		
		Transaction transaction = Cat.newTransaction(CatConstants.TYPE_LEASE_RENEW_CONSUMER,
		      tpg.getTopic() + ":" + tpg.getPartition() + ":" + tpg.getGroupId());
		transaction.addData("host", params.get("host"));
		transaction.addData("timeDiff", timeDiff);

		try {
			if (!m_clusterStateHolder.isConnected() || !m_consumerLeaseHolder.inited()) {
				response = new LeaseAcquireResponse(false, null, m_systemClockService.now()
				      + CLUSTER_NOT_READY_DELAY_TIME_MILLIS);
			} else {
				try {
					LeaseAcquireResponse leaseAcquireResponse = null;
					Transaction proxyTx = Cat.newTransaction(CatConstants.TYPE_LEASE_PROXY, "ConsumerRenew");
					try {
						leaseAcquireResponse = proxyConsumerLeaseRequestIfNecessary(req, tpg.getTopic(), "/consumer/renew",
						      params, tpg, proxyTx);
						proxyTx.setStatus(Transaction.SUCCESS);
					} catch (Exception e) {
						proxyTx.setStatus(e);
					} finally {
						proxyTx.complete();
					}

					if (leaseAcquireResponse == null) {
						if (m_clusterStateHolder.isLeaseAssigning()) {
							ConsumerLeaseAllocator leaseAllocator = m_consumerLeaseAllocatorLocator.findAllocator(
							      tpg.getTopic(), tpg.getGroupId());
							if (leaseAllocator != null) {
								response = leaseAllocator.tryRenewLease(tpg, sessionId, leaseId, getRemoteAddr(host, req));
							} else {
								response = new LeaseAcquireResponse(false, null, m_systemClockService.now()
								      + NO_STRATEGY_DELAY_TIME_MILLIS);
							}
						} else {
							response = new LeaseAcquireResponse(false, null, m_systemClockService.now()
							      + CLUSTER_NO_LEASE_ASSIGNING_DELAY_TIME_MILLIS);
						}
					} else {
						response = leaseAcquireResponse;
					}
				} catch (Exception e) {
					response = new LeaseAcquireResponse(false, null, m_systemClockService.now()
					      + EXCEPTION_CAUGHT_DELAY_TIME_MILLIS);
					transaction.setStatus(e);
				}
			}
			transaction.setStatus(Transaction.SUCCESS);
		} finally {
			if (traceLog.isInfoEnabled()) {
				traceLog.info("[ACCESS]renew consumer lease. req:{}, tpg:{} resp({})", JSON.toJSONString(params),
				      JSON.toJSONString(tpg), JSON.toJSONString(response));
			}
			if (response.isAcquired()) {
				Cat.logEvent(CatConstants.TYPE_LEASE_RENEWED_CONSUMER, tpg.getTopic() + ":" + tpg.getPartition() + ":"
				      + tpg.getGroupId(), Message.SUCCESS, "host=" + params.get("host"));
			}

			transaction.complete();
		}
		return response.toClientLeaseResponse(timeDiff);

	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Path("broker/acquire")
	public LeaseAcquireResponse tryAcquireBrokerLease(//
	      @QueryParam("topic") String topic,//
	      @QueryParam("partition") int partition,//
	      @QueryParam("sessionId") String sessionId,//
	      @QueryParam("brokerPort") int port, //
	      @QueryParam("host") @DefaultValue("-") String host,// FIXME use empty string as default value
	      @QueryParam("currentTimestamp") @DefaultValue("-1") long clientCurrentTimestamp,//
	      @Context HttpServletRequest req) {

		long timeDiff = getClientTimeDiff(clientCurrentTimestamp);

		LeaseAcquireResponse response = null;

		Map<String, String> params = new HashMap<>();
		params.put("topic", topic);
		params.put("partition", Integer.toString(partition));
		params.put("sessionId", sessionId);
		params.put("brokerPort", Integer.toString(port));
		params.put("host", getRemoteAddr(host, req));

		Transaction transaction = Cat.newTransaction(CatConstants.TYPE_LEASE_ACQUIRE_BROKER, topic + ":" + partition);
		transaction.addData("host", params.get("host"));
		transaction.addData("timeDiff", timeDiff);
		
		try {
			if (!m_clusterStateHolder.isConnected() || !m_brokerLeaseHolder.inited()) {
				response = new LeaseAcquireResponse(false, null, m_systemClockService.now()
				      + CLUSTER_NOT_READY_DELAY_TIME_MILLIS);
			} else {
				try {
					LeaseAcquireResponse leaseAcquireResponse = null;
					Transaction proxyTx = Cat.newTransaction(CatConstants.TYPE_LEASE_PROXY, "BrokerAcquire");
					try {
						leaseAcquireResponse = proxyBrokerLeaseRequestIfNecessary(req, "/broker/acquire", params, null,
						      proxyTx);
						proxyTx.setStatus(Transaction.SUCCESS);
					} catch (Exception e) {
						proxyTx.setStatus(e);
					} finally {
						proxyTx.complete();
					}

					if (leaseAcquireResponse == null) {
						if (m_clusterStateHolder.isLeaseAssigning()) {
							response = m_brokerLeaseAllocator.tryAcquireLease(topic, partition, sessionId,
							      getRemoteAddr(host, req), port);
						} else {
							response = new LeaseAcquireResponse(false, null, m_systemClockService.now()
							      + CLUSTER_NO_LEASE_ASSIGNING_DELAY_TIME_MILLIS);
						}
					} else {
						response = leaseAcquireResponse;
					}
				} catch (Exception e) {
					response = new LeaseAcquireResponse(false, null, m_systemClockService.now()
					      + EXCEPTION_CAUGHT_DELAY_TIME_MILLIS);
					transaction.setStatus(e);
				}
			}
			transaction.setStatus(Transaction.SUCCESS);
		} finally {
			if (traceLog.isInfoEnabled()) {
				traceLog.info("[ACCESS]acquire broker lease. req:{}, resp({})", JSON.toJSONString(params),
				      JSON.toJSONString(response));
			}
			if (response.isAcquired()) {
				Cat.logEvent(CatConstants.TYPE_LEASE_ACQUIRED_BROKER, topic + ":" + partition, Message.SUCCESS, "host="
				      + params.get("host"));
			}

			transaction.complete();
		}
		
		return response.toClientLeaseResponse(timeDiff);
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Path("broker/renew")
	public LeaseAcquireResponse tryRenewBrokerLease(//
	      @QueryParam("topic") String topic,//
	      @QueryParam("partition") int partition, //
	      @QueryParam("leaseId") long leaseId,//
	      @QueryParam("sessionId") String sessionId,//
	      @QueryParam("brokerPort") int port,//
	      @QueryParam("host") @DefaultValue("-") String host,//
	      @QueryParam("currentTimestamp") @DefaultValue("-1") long clientCurrentTimestamp,//
	      @Context HttpServletRequest req) {

		long timeDiff = getClientTimeDiff(clientCurrentTimestamp);

		LeaseAcquireResponse response = null;

		Map<String, String> params = new HashMap<>();
		params.put("topic", topic);
		params.put("partition", Integer.toString(partition));
		params.put("leaseId", Long.toString(leaseId));
		params.put("sessionId", sessionId);
		params.put("brokerPort", Integer.toString(port));
		params.put("host", getRemoteAddr(host, req));
		

		Transaction transaction = Cat.newTransaction(CatConstants.TYPE_LEASE_RENEW_BROKER, topic + ":" + partition);
		transaction.addData("host", params.get("host"));
		transaction.addData("timeDiff", timeDiff);

		try {
			if (!m_clusterStateHolder.isConnected() || !m_brokerLeaseHolder.inited()) {
				response = new LeaseAcquireResponse(false, null, m_systemClockService.now()
				      + CLUSTER_NOT_READY_DELAY_TIME_MILLIS);
			} else {
				try {
					LeaseAcquireResponse leaseAcquireResponse = null;
					Transaction proxyTx = Cat.newTransaction(CatConstants.TYPE_LEASE_PROXY, "BrokerRenew");
					try {
						leaseAcquireResponse = proxyBrokerLeaseRequestIfNecessary(req, "/broker/renew", params, null, proxyTx);
						proxyTx.setStatus(Transaction.SUCCESS);
					} catch (Exception e) {
						proxyTx.setStatus(e);
					} finally {
						proxyTx.complete();
					}

					if (leaseAcquireResponse == null) {
						if (m_clusterStateHolder.isLeaseAssigning()) {
							response = m_brokerLeaseAllocator.tryRenewLease(topic, partition, sessionId, leaseId,
							      getRemoteAddr(host, req), port);
						} else {
							response = new LeaseAcquireResponse(false, null, m_systemClockService.now()
							      + CLUSTER_NO_LEASE_ASSIGNING_DELAY_TIME_MILLIS);
						}
					} else {
						response = leaseAcquireResponse;
					}
				} catch (Exception e) {
					response = new LeaseAcquireResponse(false, null, m_systemClockService.now()
					      + EXCEPTION_CAUGHT_DELAY_TIME_MILLIS);
					transaction.setStatus(e);
				}
			}
			transaction.setStatus(Transaction.SUCCESS);
		} finally {
			if (traceLog.isInfoEnabled()) {
				traceLog.info("[ACCESS]renew broker lease. req:{}, resp({})", JSON.toJSONString(params),
				      JSON.toJSONString(response));
			}
			if (response.isAcquired()) {
				Cat.logEvent(CatConstants.TYPE_LEASE_RENEWED_BROKER, topic + ":" + partition, Message.SUCCESS, "host="
				      + params.get("host"));
			}

			transaction.complete();
		}

		return response.toClientLeaseResponse(timeDiff);
	}

	private LeaseAcquireResponse proxyBrokerLeaseRequestIfNecessary(HttpServletRequest req, String uri,
	      Map<String, String> params, Object payload, Transaction tx) {
		if (m_clusterStateHolder.getRole() == Role.LEADER) {
			return null;
		} else {
			if (!isFromAnotherMetaServer(req)) {
				HostPort leader = m_clusterStateHolder.getLeader();
				if (leader != null) {
					tx.addData("dest", leader.getHost() + ":" + leader.getPort());
					return proxyPass(leader.getHost(), leader.getPort(), uri, params, payload);
				} else {
					return new LeaseAcquireResponse(false, null, m_systemClockService.now()
					      + PROXY_PASS_FAIL_DELAY_TIME_MILLIS);
				}
			} else {
				return new LeaseAcquireResponse(false, null, m_systemClockService.now() + PROXY_PASS_FAIL_DELAY_TIME_MILLIS);
			}
		}
	}

	private LeaseAcquireResponse proxyConsumerLeaseRequestIfNecessary(HttpServletRequest req, String topic, String uri,
	      Map<String, String> params, Object payload, Transaction tx) {

		Map<String, ClientContext> responsors = m_metaServerAssignmentHolder.getAssignment(topic);

		if (responsors != null && !responsors.isEmpty()) {
			ClientContext responsor = responsors.values().iterator().next();
			if (responsor != null) {
				if (m_config.getMetaServerHost().equals(responsor.getIp())
				      && m_config.getMetaServerPort() == responsor.getPort()) {
					return null;
				} else {
					if (!isFromAnotherMetaServer(req)) {
						tx.addData("dest", responsor.getIp() + ":" + responsor.getPort());
						return proxyPass(responsor.getIp(), responsor.getPort(), uri, params, payload);
					} else {
						return new LeaseAcquireResponse(false, null, m_systemClockService.now()
						      + PROXY_PASS_FAIL_DELAY_TIME_MILLIS);
					}
				}
			}
		}
		return new LeaseAcquireResponse(false, null, m_systemClockService.now() + NO_ASSIGNMENT_DELAY_TIME_MILLIS);

	}

	private boolean isFromAnotherMetaServer(HttpServletRequest req) {
		String proxyHeader = req.getHeader(HEADER_PROXY_KEY);
		return StringUtils.equals(proxyHeader, HEADER_PROXY_VALUE);
	}
	
	private long getClientTimeDiff(long clientTimestamp) {
		if (clientTimestamp == -1) {
			return Long.MIN_VALUE;
		}
		return clientTimestamp - System.currentTimeMillis();
	}

	private LeaseAcquireResponse proxyPass(String host, int port, String uri, Map<String, String> params, Object payload) {
		uri = "/lease" + uri;
		if (log.isDebugEnabled()) {
			log.debug("Proxy pass request to http://{}:{}{}(params={}, payload={})", host, port, uri, params,
			      JSON.toJSONString(payload));
		}
		try {
			URIBuilder uriBuilder = new URIBuilder()//
			      .setScheme("http")//
			      .setHost(host)//
			      .setPort(port)//
			      .setPath(uri);

			if (params != null) {
				for (Map.Entry<String, String> entry : params.entrySet()) {
					uriBuilder.addParameter(entry.getKey(), entry.getValue());
				}
			}

			HttpResponse response = post(uriBuilder.build().toURL(), payload);

			if (response.getStatusCode() == HttpStatus.SC_OK && response.hasResponseContent()) {
				String responseContent = new String(response.getRespContent(), "UTF-8");
				if (!StringUtils.isBlank(responseContent)) {
					return JSON.parseObject(responseContent, LeaseAcquireResponse.class);
				} else {
					return new LeaseAcquireResponse(false, null, m_systemClockService.now()
					      + PROXY_PASS_FAIL_DELAY_TIME_MILLIS);
				}
			} else {
				if (log.isDebugEnabled()) {
					log.debug("Response error while proxy passing to http://{}:{}{}.(status={}}).", host, port, uri,
					      response.getStatusCode());
				}
				return new LeaseAcquireResponse(false, null, m_systemClockService.now() + PROXY_PASS_FAIL_DELAY_TIME_MILLIS);
			}

		} catch (Exception e) {
			// ignore
			if (log.isDebugEnabled()) {
				log.debug("Failed to proxy pass to http://{}:{}{}.", host, port, uri, e);
			}
			return new LeaseAcquireResponse(false, null, m_systemClockService.now() + PROXY_PASS_FAIL_DELAY_TIME_MILLIS);
		}

	}

	private HttpResponse post(URL url, Object payload) throws IOException {
		HttpResponse response = new HttpResponse();
		HttpURLConnection conn = null;
		try {
			conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("POST");
			conn.addRequestProperty(HEADER_PROXY_KEY, HEADER_PROXY_VALUE);
			conn.addRequestProperty("Content-type", ContentType.APPLICATION_JSON.toString());
			conn.setConnectTimeout(m_config.getProxyPassConnectTimeout());
			conn.setReadTimeout(m_config.getProxyPassReadTimeout());
			if (payload != null) {
				conn.setDoOutput(true);
			}
			conn.connect();

			if (payload != null) {
				OutputStream out = null;

				try {
					out = conn.getOutputStream();
					out.write(JSON.toJSONString(payload).getBytes());
					out.flush();

				} finally {
					if (out != null) {
						try {
							out.close();
						} catch (Exception e) {
							// ignore;
						}
					}
				}
			}

			InputStream is = null;

			try {
				is = conn.getInputStream();
				response.setRespContent(IO.INSTANCE.readFrom(is));
			} finally {
				if (is != null) {
					try {
						is.close();
					} catch (Exception e) {
						// ignore;
					}
				}
			}

			response.setStatusCode(conn.getResponseCode());

		} finally {
			if (conn != null) {
				try {
					conn.disconnect();
				} catch (Exception e) {
					// ignore;
				}
			}
		}

		return response;
	}

	private static class HttpResponse {
		private int statusCode = -1;

		private byte[] respContent;

		public int getStatusCode() {
			return statusCode;
		}

		public void setStatusCode(int statusCode) {
			this.statusCode = statusCode;
		}

		public boolean hasResponseContent() {
			return respContent != null && respContent.length > 0;
		}

		public byte[] getRespContent() {
			return respContent;
		}

		public void setRespContent(byte[] respContent) {
			this.respContent = respContent;
		}

	}

	private String getRemoteAddr(String host, HttpServletRequest req) {
		return "-".equals(host) ? req.getRemoteAddr() : host;
	}
}
