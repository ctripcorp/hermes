package com.ctrip.hermes.core.meta.remote;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.helper.Files.IO;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.net.Networks;
import org.unidal.tuple.Pair;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.config.CoreConfig;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;
import com.ctrip.hermes.core.meta.internal.MetaProxy;
import com.ctrip.hermes.meta.entity.Meta;
import com.google.common.base.Charsets;
import com.google.common.base.Function;

@Named(type = MetaProxy.class, value = RemoteMetaProxy.ID)
public class RemoteMetaProxy implements MetaProxy {

	private static final String HOST = "host";

	private static final String BROKER_PORT = "brokerPort";

	private static final Logger log = LoggerFactory.getLogger(RemoteMetaProxy.class);

	private static final String LEASE_ID = "leaseId";

	private static final String SESSION_ID = "sessionId";

	private static final String TOPIC = "topic";

	private static final String PARTITION = "partition";

	private static final String CURRENT_TIMESTAMP = "currentTimestamp";

	public final static String ID = "remote";

	@Inject
	private MetaServerLocator m_metaServerLocator;

	@Inject
	private CoreConfig m_config;

	@Override
	public LeaseAcquireResponse tryAcquireConsumerLease(Tpg tpg, String sessionId) {
		Map<String, String> params = new HashMap<String, String>();
		params.put(SESSION_ID, sessionId);
		params.put(HOST, Networks.forIp().getLocalHostAddress());
		params.put(CURRENT_TIMESTAMP, String.valueOf(System.currentTimeMillis()));
		
		String response = post("/lease/consumer/acquire", params, tpg);
		if (response != null) {
			return JSON.parseObject(response, LeaseAcquireResponse.class);
		} else {
			if (log.isDebugEnabled()) {
				log.debug("No response while posting meta server[tryAcquireConsumerLease]");
			}
			return null;
		}
	}

	@Override
	public LeaseAcquireResponse tryRenewConsumerLease(Tpg tpg, Lease lease, String sessionId) {
		Map<String, String> params = new HashMap<String, String>();
		params.put(LEASE_ID, String.valueOf(lease.getId()));
		params.put(SESSION_ID, sessionId);
		params.put(HOST, Networks.forIp().getLocalHostAddress());
		params.put(CURRENT_TIMESTAMP, String.valueOf(System.currentTimeMillis()));

		String response = post("/lease/consumer/renew", params, tpg);
		if (response != null) {
			return JSON.parseObject(response, LeaseAcquireResponse.class);
		} else {
			if (log.isDebugEnabled()) {
				log.debug("No response while posting meta server[tryRenewConsumerLease]");
			}
			return null;
		}
	}

	@Override
	public LeaseAcquireResponse tryRenewBrokerLease(String topic, int partition, Lease lease, String sessionId,
	      int brokerPort) {
		Map<String, String> params = new HashMap<String, String>();
		params.put(LEASE_ID, String.valueOf(lease.getId()));
		params.put(SESSION_ID, sessionId);
		params.put(TOPIC, topic);
		params.put(PARTITION, Integer.toString(partition));
		params.put(BROKER_PORT, String.valueOf(brokerPort));
		params.put(HOST, Networks.forIp().getLocalHostAddress());
		params.put(CURRENT_TIMESTAMP, String.valueOf(System.currentTimeMillis()));

		String response = post("/lease/broker/renew", params, null);
		if (response != null) {
			return JSON.parseObject(response, LeaseAcquireResponse.class);
		} else {
			if (log.isDebugEnabled()) {
				log.debug("No response while posting meta server[tryRenewBrokerLease]");
			}
			return null;
		}
	}

	@Override
	public LeaseAcquireResponse tryAcquireBrokerLease(String topic, int partition, String sessionId, int brokerPort) {
		Map<String, String> params = new HashMap<String, String>();
		params.put(SESSION_ID, sessionId);
		params.put(TOPIC, topic);
		params.put(PARTITION, Integer.toString(partition));
		params.put(BROKER_PORT, String.valueOf(brokerPort));
		params.put(HOST, Networks.forIp().getLocalHostAddress());
		params.put(CURRENT_TIMESTAMP, String.valueOf(System.currentTimeMillis()));

		String response = post("/lease/broker/acquire", params, null);
		if (response != null) {
			return JSON.parseObject(response, LeaseAcquireResponse.class);
		} else {
			if (log.isDebugEnabled()) {
				log.debug("No response while posting meta server[tryAcquireBrokerLease]");
			}
			return null;
		}
	}

	private Pair<Integer, String> pollMetaServer(Function<String, Pair<Integer, String>> fun) {
		List<String> metaServerIpPorts = m_metaServerLocator.getMetaServerList();

		for (String ipPort : metaServerIpPorts) {
			Pair<Integer, String> result = fun.apply(ipPort);
			if (result != null) {
				return result;
			} else {
				continue;
			}
		}

		return null;

	}

	@Override
	public Pair<Integer, String> getRequestToMetaServer(final String path, final Map<String, String> requestParams) {
		return pollMetaServer(new Function<String, Pair<Integer, String>>() {

			@Override
			public Pair<Integer, String> apply(String ip) {
				String url = String.format("http://%s%s", ip, path);
				InputStream is = null;
				try {
					if (requestParams != null) {
						String encodedRequestParamStr = encodePropertiesStr(requestParams);

						if (encodedRequestParamStr != null) {
							url = url + "?" + encodedRequestParamStr;
						}

					}

					HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();

					conn.setConnectTimeout(m_config.getMetaServerConnectTimeout());
					conn.setReadTimeout(m_config.getMetaServerReadTimeout());
					conn.setRequestMethod("GET");
					conn.connect();

					int statusCode = conn.getResponseCode();

					if (statusCode == 200) {
						is = conn.getInputStream();
						return new Pair<Integer, String>(statusCode, IO.INSTANCE.readFrom(is, Charsets.UTF_8.name()));
					} else {
						return new Pair<Integer, String>(statusCode, null);
					}

				} catch (Exception e) {
					// ignore
					if (log.isDebugEnabled()) {
						log.debug("Poll meta server error.", e);
					}
					return null;
				} finally {
					if (is != null) {
						try {
							is.close();
						} catch (Exception e) {
							// ignore it
						}
					}
				}

			}
		});
	}

	private String post(final String path, final Map<String, String> requestParams, final Object payload) {
		Pair<Integer, String> codeAndRes = pollMetaServer(new Function<String, Pair<Integer, String>>() {

			@Override
			public Pair<Integer, String> apply(String ip) {

				String url = String.format("http://%s%s", ip, path);
				InputStream is = null;
				OutputStream os = null;

				try {
					if (requestParams != null) {
						String encodedRequestParamStr = encodePropertiesStr(requestParams);

						if (encodedRequestParamStr != null) {
							url = url + "?" + encodedRequestParamStr;
						}
					}

					HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();

					conn.setConnectTimeout(m_config.getMetaServerConnectTimeout());
					conn.setReadTimeout(m_config.getMetaServerReadTimeout());
					conn.setRequestMethod("POST");
					conn.addRequestProperty("content-type", "application/json");

					if (payload != null) {
						conn.setDoOutput(true);
						conn.connect();
						os = conn.getOutputStream();
						os.write(JSON.toJSONBytes(payload));
					} else {
						conn.connect();
					}

					int statusCode = conn.getResponseCode();

					if (statusCode == 200) {
						is = conn.getInputStream();
						return new Pair<Integer, String>(statusCode, IO.INSTANCE.readFrom(is, Charsets.UTF_8.name()));
					} else {
						if (log.isDebugEnabled()) {
							log.debug("Response error while posting meta server error({url={}, status={}}).", url, statusCode);
						}
						return new Pair<Integer, String>(statusCode, null);
					}

				} catch (Exception e) {
					// ignore
					if (log.isDebugEnabled()) {
						log.debug("Post meta server error.", e);
					}
					return null;
				} finally {
					if (is != null) {
						try {
							is.close();
						} catch (Exception e) {
							// ignore it
						}
					}

					if (os != null) {
						try {
							os.close();
						} catch (Exception e) {
							// ignore it
						}
					}
				}

			}
		});
		return codeAndRes == null ? null : codeAndRes.getValue();
	}

	private String encodePropertiesStr(Map<String, String> properties) throws UnsupportedEncodingException {
		StringBuilder sb = new StringBuilder();
		for (Map.Entry<String, String> entry : properties.entrySet()) {
			sb.append(URLEncoder.encode(entry.getKey(), Charsets.UTF_8.name()))//
			      .append("=")//
			      .append(URLEncoder.encode(entry.getValue(), Charsets.UTF_8.name()))//
			      .append("&");
		}

		if (sb.length() > 0) {
			return sb.substring(0, sb.length() - 1);
		} else {
			return null;
		}
	}

	@Override
	public int registerSchema(String schema, String subject) {
		Map<String, String> params = new HashMap<String, String>();
		params.put("schema", schema);
		params.put("subject", subject);
		String response = post("/schema/register", null, params);
		if (response != null) {
			try {
				return Integer.valueOf(response);
			} catch (Exception e) {
				log.error("Can not parse response, schema: {}, subject: {}\nResponse: {}", schema, subject, response);
			}
		}
		throw new RuntimeException(String.format("Register schema %s[%s] failed.", subject, schema));
	}

	@Override
	public String getSchemaString(int schemaId) {
		Map<String, String> params = new HashMap<String, String>();
		params.put("id", String.valueOf(schemaId));
		String response = getRequestToMetaServer("/schema/register", params).getValue();
		if (response != null) {
			return response;
		} else {
			log.warn("No response while getting meta server[getSchemaString]");
		}
		return null;
	}

	@Override
	@SuppressWarnings("unchecked")
	public Map<Integer, Offset> findMessageOffsetByTime(String topic, int partition, long time) {
		Map<String, String> params = new HashMap<String, String>();
		params.put("topic", topic);
		params.put("partition", String.valueOf(partition));
		params.put("time", String.valueOf(time));
		Pair<Integer, String> responsePair = getRequestToMetaServer("/message/offset", params);
		String response = responsePair == null ? null : responsePair.getValue();
		if (response != null) {
			try {
				Map<Integer, JSONObject> map = (Map<Integer, JSONObject>) JSON.parse(response);
				if (map != null) {
					return parseFromJsonObject(map);
				}
			} catch (Exception e) {
				log.warn("Parse Offset object failed: [{}({}), {}], response:{}.", topic, partition, time, response, e);
			}
		}
		throw new RuntimeException(String.format("Find message offset failed: [%s(%s), %s], response:%s.", //
		      topic, partition, time, response));
	}

	public Map<Integer, Offset> parseFromJsonObject(Map<Integer, JSONObject> map) {
		Map<Integer, Offset> result = new HashMap<Integer, Offset>();
		for (Entry<Integer, JSONObject> entry : map.entrySet()) {
			int partitionId = Integer.valueOf(String.valueOf(entry.getKey()));
			long pOffset = Long.valueOf(entry.getValue().get("priorityOffset").toString());
			long npOffset = Long.valueOf(entry.getValue().get("nonPriorityOffset").toString());
			result.put(partitionId, new Offset(pOffset, npOffset, null));
		}
		return result;
	}

	@Override
	public Meta getTopicsMeta(Set<String> topics) {
		String response = post("/meta/topics", null, topics);
		if (response != null) {
			return JSON.parseObject(response, Meta.class);
		} else {
			if (log.isDebugEnabled()) {
				log.debug("No response while posting meta server[getTopicsMeta]");
			}
			return null;
		}
	}
}
