package com.ctrip.hermes.metaserver.rest.resource;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.http.HttpStatus;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.helper.Files.IO;

import sun.net.www.protocol.http.HttpURLConnection;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.bo.HostPort;
import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.schedule.ExponentialSchedulePolicy;
import com.ctrip.hermes.core.transport.command.QueryMessageOffsetByTimeCommand;
import com.ctrip.hermes.core.transport.command.QueryOffsetResultCommand;
import com.ctrip.hermes.core.transport.endpoint.EndpointClient;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaserver.broker.BrokerAssignmentHolder;
import com.ctrip.hermes.metaserver.broker.endpoint.MetaEndpointClient;
import com.ctrip.hermes.metaserver.cluster.ClusterStateHolder;
import com.ctrip.hermes.metaserver.cluster.Role;
import com.ctrip.hermes.metaserver.commons.Assignment;
import com.ctrip.hermes.metaserver.commons.ClientContext;
import com.ctrip.hermes.metaserver.config.MetaServerConfig;
import com.ctrip.hermes.metaserver.meta.MetaHolder;
import com.ctrip.hermes.metaserver.monitor.QueryOffsetResultMonitor;
import com.ctrip.hermes.metaserver.rest.commons.RestException;
import com.google.common.util.concurrent.SettableFuture;

@Path("/message/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class MessageAssistResource {
	private static final Logger log = LoggerFactory.getLogger(MessageAssistResource.class);

	private static final String HEADER_PROXY_KEY = "hmProxy";

	private static final String HEADER_PROXY_VALUE = "true";

	private static final int DEFAULT_CONCURRENT_LEVEL = 30;

	private static final AtomicInteger m_singals = new AtomicInteger(DEFAULT_CONCURRENT_LEVEL);

	private BrokerAssignmentHolder m_brokerAssignments = PlexusComponentLocator.lookup(BrokerAssignmentHolder.class);

	private MetaHolder m_metaHolder = PlexusComponentLocator.lookup(MetaHolder.class);

	private EndpointClient m_endpointClient = PlexusComponentLocator.lookup(EndpointClient.class, MetaEndpointClient.ID);

	private MetaServerConfig m_config = PlexusComponentLocator.lookup(MetaServerConfig.class);

	private ExecutorService m_offsetQueryExecutor = Executors.newFixedThreadPool(3);

	private ClusterStateHolder m_clusterStateHolder = PlexusComponentLocator.lookup(ClusterStateHolder.class);

	private QueryOffsetResultMonitor m_monitor = PlexusComponentLocator.lookup(QueryOffsetResultMonitor.class);

	@GET
	@Path("offset")
	public Response queryMessageIdByTime( //
	      @QueryParam("topic") String topic, //
	      @QueryParam("partition") @DefaultValue("-1") int partition, //
	      @QueryParam("time") @DefaultValue("-1") long time,//
	      @Context HttpServletRequest req) {
		try {
			if (m_singals.decrementAndGet() < 0) {
				return Response.status(509).entity("Too many concurrent requests.").build();
			}

			time = -1 == time ? Long.MAX_VALUE : -2 == time ? Long.MIN_VALUE : time;

			Map<Integer, Offset> result = null;
			if (m_clusterStateHolder.getRole() == Role.LEADER) {
				result = findOffsetFromBroker(topic, partition, time);
			} else if (!isFromAnotherMetaServer(req)) {
				Map<String, String> params = new HashMap<String, String>();
				params.put("topic", topic);
				params.put("partition", String.valueOf(partition));
				params.put("time", String.valueOf(time));

				HostPort leader = m_clusterStateHolder.getLeader();
				if (leader != null) {
					result = findOffsetFromMetaLeader(leader.getHost(), leader.getPort(), params);
				}
			}

			if (result == null || result.size() == 0) {
				return Response.status(Status.NOT_FOUND)
				      .entity(String.format("No offset found for [%s, %s, %s]", topic, partition, time)).build();
			}

			return Response.status(Status.OK).entity(result).build();
		} finally {
			m_singals.incrementAndGet();
		}
	}

	private boolean isFromAnotherMetaServer(HttpServletRequest req) {
		String proxyHeader = req.getHeader(HEADER_PROXY_KEY);
		return StringUtils.equals(proxyHeader, HEADER_PROXY_VALUE);
	}

	private Map<Integer, Offset> findOffsetFromBroker(final String topicName, final int partition, final long time) {
		final Map<Integer, Offset> result = new ConcurrentHashMap<Integer, Offset>();
		Topic topic = m_metaHolder.getMeta().findTopic(topicName);
		if (topic != null) {
			try {
				final Assignment<Integer> assignment = m_brokerAssignments.getAssignment(topicName);

				if (assignment == null) {
					return null;
				}

				List<Integer> partitions = new ArrayList<>(topic.getPartitions().size());

				if (partition >= 0 && topic.findPartition(partition) != null) {
					partitions.add(partition);
				} else if (partition == -1) {
					for (Partition p : topic.getPartitions()) {
						partitions.add(p.getId());
					}
				}

				if (!partitions.isEmpty()) {
					final CountDownLatch latch = new CountDownLatch(partitions.size());
					for (Integer partitionId : partitions) {
						final int id = partitionId;
						m_offsetQueryExecutor.execute(new Runnable() {
							@Override
							public void run() {
								try {
									Map<String, ClientContext> partitionAssignment = assignment.getAssignment(id);
									if (partitionAssignment != null && partitionAssignment.size() > 0) {
										ClientContext client = partitionAssignment.entrySet().iterator().next().getValue();
										Offset offset = findOffsetByTime(topicName, id, time, getBrokerEndpoint(client));
										if (offset != null) {
											result.put(id, offset);
										}
									}
								} catch (Exception e) {
									log.warn("Query message offset failed: {}:{} {}", topicName, partition, time, e);
								} finally {
									latch.countDown();
								}
							}
						});
					}
					latch.await(m_config.getQueryMessageOffsetTimeoutMillis(), TimeUnit.MILLISECONDS);
				}

				if (result.size() == partitions.size()) {
					return result;
				}
			} catch (Exception e) {
				log.warn("Query message offset failed: {}:{} {}", topicName, partition, time, e);
			}
			throw new RestException("Query message offset failed.", Status.INTERNAL_SERVER_ERROR);
		} else {
			throw new RestException(String.format("Topic %s not found.", topicName), Status.NOT_FOUND);
		}
	}

	private Endpoint getBrokerEndpoint(ClientContext context) {
		return new Endpoint()//
		      .setId(context.getName()) //
		      .setType(Endpoint.BROKER) //
		      .setHost(context.getIp()) //
		      .setPort(context.getPort());
	}

	private Offset findOffsetByTime(String topic, int partition, long time, Endpoint endpoint) throws Exception {
		long timeout = m_config.getQueryMessageOffsetTimeoutMillis();
		ExponentialSchedulePolicy schedulePolicy = new ExponentialSchedulePolicy(500, (int) timeout);

		long expire = System.currentTimeMillis() + timeout;
		while (!Thread.interrupted() && System.currentTimeMillis() < expire) {
			SettableFuture<QueryOffsetResultCommand> future = SettableFuture.create();
			QueryMessageOffsetByTimeCommand cmd = new QueryMessageOffsetByTimeCommand(topic, partition, time);
			cmd.setFuture(future);

			m_monitor.monitor(cmd);
			if (m_endpointClient.writeCommand(endpoint, cmd)) {
				QueryOffsetResultCommand resultCmd = null;
				try {
					resultCmd = future.get(timeout, TimeUnit.MILLISECONDS);
					if (resultCmd != null && resultCmd.getOffset() != null) {
						return resultCmd.getOffset();
					} else {
						schedulePolicy.fail(true);
					}
				} finally {
					m_monitor.remove(cmd);
				}
			} else {
				m_monitor.remove(cmd);
				schedulePolicy.fail(true);
			}
		}
		throw new RuntimeException("Find offset by time failed [Query Time Expired].");
	}

	@SuppressWarnings("unchecked")
	private Map<Integer, Offset> findOffsetFromMetaLeader(String host, int port, Map<String, String> params) {
		if (log.isDebugEnabled()) {
			log.debug("Proxy pass find-offset request to {}:{} (params={})", host, port, params);
		}
		try {
			URIBuilder uriBuilder = new URIBuilder()//
			      .setScheme("http")//
			      .setHost(host)//
			      .setPort(port)//
			      .setPath("/message/offset");

			if (params != null) {
				for (Map.Entry<String, String> entry : params.entrySet()) {
					uriBuilder.addParameter(entry.getKey(), entry.getValue());
				}
			}

			HttpResponse response = get(uriBuilder.build().toURL());

			if (response != null && response.getStatusCode() == HttpStatus.SC_OK && response.hasResponseContent()) {
				String responseContent = new String(response.getRespContent(), "UTF-8");
				if (!StringUtils.isBlank(responseContent)) {
					return (Map<Integer, Offset>) JSON.parse(responseContent);
				}
			} else {
				if (log.isDebugEnabled()) {
					log.debug("Response error while proxy passing to {}:{}(status={}}).", host, port,
					      response.getStatusCode());
				}
			}
		} catch (Exception e) {
			if (log.isDebugEnabled()) {
				log.debug("Failed to proxy pass to http://{}:{}.", host, port, e);
			}
		}
		return null;
	}

	private HttpResponse get(URL url) throws IOException {
		HttpResponse response = new HttpResponse();
		HttpURLConnection conn = null;
		try {
			conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.addRequestProperty(HEADER_PROXY_KEY, HEADER_PROXY_VALUE);
			conn.addRequestProperty("Content-type", ContentType.APPLICATION_JSON.toString());
			conn.setConnectTimeout(m_config.getProxyPassConnectTimeout());
			conn.setReadTimeout(m_config.getProxyPassReadTimeout());

			conn.connect();

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

			response.setStatsCode(conn.getResponseCode());

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
		private int statsCode = -1;

		private byte[] respContent;

		public int getStatusCode() {
			return statsCode;
		}

		public void setStatsCode(int statsCode) {
			this.statsCode = statsCode;
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
}
