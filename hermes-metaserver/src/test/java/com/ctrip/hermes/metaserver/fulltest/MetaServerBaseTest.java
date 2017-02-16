package com.ctrip.hermes.metaserver.fulltest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;

import org.apache.curator.test.TestingServer;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.util.EntityUtils;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.ComponentTestCase;
import org.unidal.net.Networks;
import org.xml.sax.SAXException;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.ctrip.hermes.core.bo.HostPort;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.meta.transform.DefaultSaxParser;
import com.ctrip.hermes.meta.transform.DefaultXmlBuilder;
import com.ctrip.hermes.metaserver.cluster.Role;
import com.ctrip.hermes.metaserver.commons.Assignment;
import com.ctrip.hermes.metaserver.commons.ClientContext;
import com.ctrip.hermes.metaserver.commons.MetaServerStatusResponse;
import com.ctrip.hermes.metaservice.service.ZookeeperService;
import com.ctrip.hermes.metaservice.zk.ZKClient;
import com.ctrip.hermes.metaservice.zk.ZKPathUtils;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MetaServerBaseTest extends ComponentTestCase {
	private static final Logger logger = LoggerFactory.getLogger(MetaServerBaseTest.class);

	public static final String metaXmlFile = "MetaServerTest.xml";

	public static final String metaXmlFileBackup = "MetaServerTest_Backup.xml";

	private ZookeeperService zkService;

	private ZKClient m_client;

	private TestingServer m_zkServer;

	/*
	 * initial value is based on "meta-port" in hermes.properties.
	 */
	protected static int portPointer = 1248;

	protected static List<Integer> ports = new ArrayList<>();

	protected static AtomicInteger zkVersion = new AtomicInteger(10000);

	protected static AtomicInteger brokerPort = new AtomicInteger(5000);

	protected Map<Integer/* port */, HermesClassLoaderMetaServer/*
																					 * meta server
																					 */> metaServers = new HashMap<>();

	protected Map<Integer/* port */, ServiceDiscovery<Void>/*
																			 * broker serviceDiscovery
																			 */> tempBrokers = new HashMap<>();

	protected Stack<Topic> tempTopics = new Stack<>();

	protected static String localhostIP = Networks.forIp().getLocalHostAddress();

	@Before
	public void setUp() throws Exception {
		cleanEnv();

		startZKServer();
		initMetaXmlAndService();
		setupZKNodes();
	}

	private void cleanEnv() throws Exception {
		portPointer = 1248;
		ports = new ArrayList<>();
		zkVersion = new AtomicInteger(10000);
		brokerPort = new AtomicInteger(5000);

		metaServers.clear();
		tempBrokers.clear();
	}

	@After
	public void tearDown() throws Exception {
		// close all meta servers anyway
		stopMultipleMetaServersRandomly(Integer.MAX_VALUE);
		if (null != m_zkServer) {
			Thread.sleep(2000);
			m_zkServer.close();
			logger.info("==ZKServer== Zookeeper Server Closed");
		} else {
			logger.info("==ZKServer== Zookeeper is Null. No Need to Close.");
		}

		super.tearDown();
	}

	public void initPlexus() throws Exception {
		super.setUp();
	}

	public void initMetaXmlAndService() throws Exception {
		MetaHelper.outputToTestXml(MetaHelper.loadMeta(metaXmlFileBackup));

		zkService = PlexusComponentLocator.lookup(ZookeeperService.class);
		m_client = PlexusComponentLocator.lookup(ZKClient.class);
	}

	protected void startMetaServer(int port) throws Exception {
		HermesClassLoaderMetaServer metaServer = new HermesClassLoaderMetaServer(port);
		metaServer.startServer();

		metaServers.put(port, metaServer);
	}

	protected void stopMetaServer(int port) throws Exception {
		HermesClassLoaderMetaServer metaServer = metaServers.get(port);
		if (metaServer == null) {
			throw new Exception(String.format("MetaServer: localhost:%d, not found!", port));
		} else {
			metaServer.stopServer();
			metaServers.remove(port);
		}
	}

	private void setupZKNodes() throws Exception {
		ZookeeperService zkService = PlexusComponentLocator.lookup(ZookeeperService.class);

		Meta meta = new MockMetaService().refreshMeta();
		zkService.updateZkBaseMetaVersion(meta.getVersion());

		zkService.updateZkBaseMetaVersion(MetaServerBaseTest.zkVersion.getAndIncrement());

		zkService.ensurePath(ZKPathUtils.getMetaInfoZkPath());
		zkService.ensurePath(ZKPathUtils.getMetaServerAssignmentRootZkPath());
	}

	protected void startMultipleMetaServers(int number) throws Exception {
		for (int i = 0; i < number; i++) {
			startMetaServer(portPointer);
			ports.add(portPointer);
			portPointer++;
		}
	}

	protected void stopMultipleMetaServersRandomly(int number) throws Exception {
		int closeNumber = getRightCloseNumber(number);
		for (int i = 0; i < closeNumber; i++) {
			int randomIndex = new Random().nextInt(ports.size());
			int randomPort = ports.get(randomIndex);
			stopMetaServer(randomPort);
			ports.remove(randomIndex);
			logger.info("==StopMetaServer== Trying to Stop Meta Server : {}. Remaining are {}", randomPort, ports);
		}
	}

	private int getRightCloseNumber(int number) {
		int closeNumber = number;
		if (number > metaServers.size()) {
			logger.warn("Want to stop {} metaservsers, but only {} running. Will close them all.", number,
			      metaServers.size());
			closeNumber = metaServers.size();
		}
		return closeNumber;
	}

	protected void startZKServer() throws Exception {
		startZKServer(2181);
	}

	protected void startZKServer(int port) throws Exception {
		try {
			m_zkServer = new TestingServer(port);
			m_zkServer.restart();
			logger.info("==Start ZKServer== on port {} Success.", port);

		} catch (java.net.BindException e) {
			logger.warn("==Start ZKServer== on port {} failed. BindException.", port);
		}
	}

	protected void assertOnlyOneLeader() throws InterruptedException, IOException {
		Thread.sleep(5000);
		List<String> expectedServers = buildExpectedServers(metaServers);

		// 1. check /metaservers/server for all metaservers
		for (Map.Entry<Integer, HermesClassLoaderMetaServer> entry : metaServers.entrySet()) {
			assertListEqual(expectedServers, BaseRestClient.getMetaServers(entry.getKey()));
		}

		// 2. check /metaservers/status for the right and only one leader
		int leaderCount = 0;
		HostPort leader = null;
		HostPort trueLeader = null;
		for (Map.Entry<Integer, HermesClassLoaderMetaServer> entry : metaServers.entrySet()) {
			MetaServerStatusResponse re = BaseRestClient.getMetaStatusLeaderInfo(entry.getKey());
			if (re.getRole() == Role.LEADER) {
				leaderCount++;
				trueLeader = re.getLeaderInfo();
				leader = re.getLeaderInfo();
			} else {
				if (leader != null) {
					assertEquals(leader, re.getLeaderInfo());
				} else {
					leader = re.getLeaderInfo();
				}
			}
		}

		assertEquals(1, leaderCount);
		assertEquals(leader, trueLeader);
		logger.info("==LeaderShip== Only One Leader {} Among {} Meta Servers", trueLeader.getPort(), metaServers.size());
	}

	protected void assertAllMetaServerHaveTopic(Topic topic) {
		for (Map.Entry<Integer, HermesClassLoaderMetaServer> entry : metaServers.entrySet()) {
			Meta meta = BaseRestClient.getMeta(entry.getKey());
			Map<String, Topic> topics = meta.getTopics();

			assertTrue(topics.containsKey(topic.getName()));
			assertTrue(topics.containsValue(topic));
		}
	}

	protected void assertAllMetaServerHaveNotTopic(Topic topic) {
		for (Map.Entry<Integer, HermesClassLoaderMetaServer> entry : metaServers.entrySet()) {
			Meta meta = BaseRestClient.getMeta(entry.getKey());
			Map<String, Topic> topics = meta.getTopics();

			assertFalse(topics.containsKey(topic.getName()));
			assertFalse(topics.containsValue(topic));
		}
	}

	protected void assertBrokerAssignmentRight() throws Exception {
		// wait a while to assign. set to 2 second for slow machine in CI.
		Thread.sleep(2000);

		List<Topic> topics = new ArrayList<>(MetaHelper.loadMeta().getTopics().values());

		for (Integer port : metaServers.keySet()) {
			MetaServerStatusResponse re = BaseRestClient.getMetaStatusBrokerAssignment(port);

			// only Leader has Broker Assignment Info.
			if (re.getRole() == Role.LEADER) {
				if (tempBrokers.size() > 0) { // means at lease one broker be
					// assigned.
					assertEquals("Topics.size() should be equal to Broker Assignments", topics.size(), re
					      .getBrokerAssignments().size());
				}
				List<Integer> runningBrokers = getRunningBrokers(re.getBrokerAssignments());

				int writeBrokerCount = topics.size() < tempBrokers.keySet().size() ? topics.size() : tempBrokers.keySet()
				      .size();
				assertEquals(writeBrokerCount, runningBrokers.size());
			}
		}
	}

	private List<Integer> getRunningBrokers(Map<String, Assignment<Integer>> brokerAssignments) {
		Set<Integer> ports = new HashSet<>();
		for (Assignment<Integer> assignment : brokerAssignments.values()) {
			for (Map<String, ClientContext> map : assignment.getAssignments().values()) {
				for (Object o : map.values()) {
					Map clientContext = JSON.parseObject(JSON.toJSONString(o), Map.class);
					int port = (int) clientContext.get("port");
					ports.add(port);
				}
			}
		}
		return new ArrayList<>(ports);
	}

	protected void assertMetaServerAssignmentRight() throws Exception {
		Thread.sleep(500);
		List<Topic> topics = new ArrayList<>(MetaHelper.loadMeta().getTopics().values());

		for (Integer port : metaServers.keySet()) {
			MetaServerStatusResponse re = BaseRestClient.getMetaStatusMetaServerAssignment(port);
			assertEquals("Topics.size() should be equal to MetaServer Assignments", topics.size(), re
			      .getMetaServerAssignments().getAssignments().size());

			// ArrayList<Integer> runningMetaServers =
			// getRunningMetaServers(re.getMetaServerAssignments().getAssignment());
			// assertListEqual(new ArrayList<>(metaServers.keySet()),
			// runningMetaServers);
		}
	}

	private ArrayList<Integer> getRunningMetaServers(Map<String, Map<String, ClientContext>> assignment) {
		Set<Integer> ports = new HashSet<>();
		for (Map<String, ClientContext> map : assignment.values()) {

			// CAN'T cast map.values() to ClientContext, 'cause the classLoader
			// of ClientContext is HermesClassLoader!
			for (Object o : map.values()) {
				Map clientContext = JSON.parseObject(JSON.toJSONString(o), Map.class);
				int port = (int) clientContext.get("port");
				ports.add(port);
			}
		}
		return new ArrayList<>(ports);
	}

	private void assertListEqual(List list1, List list2) {
		assertEquals(list1.size(), list2.size());

		Set set1 = new HashSet(list1);
		Set set2 = new HashSet(list2);
		assertEquals(set1, set2);
	}

	// ============= Broker Lease================
	protected Lease initLeaseToBroker(int brokerPort, String topic, int partition, String sessionId) throws Exception {
		LeaseAcquireResponse re = BaseRestClient.getAcquireBrokerLease(topic, partition, sessionId, brokerPort, 1248);

		return re.getLease();
	}

	protected Lease changeLeaseTo(int fromBrokerPort, int toBrokerPort, String topic, int partition, long timeoutInMs)
	      throws Exception {
		// 方法1，用BrokerAssignmentHolder重新分配, 无法生效：因现状Reassign策略是取所有可用Broker做遍历均分。

		// 方法2，把Broker1挂掉。自动会做assignment，将Topic-Partition分给其他Broker
		tempBrokers.get(fromBrokerPort).close();
		return null;
	}

	protected void assertAcquireBrokerLeaseOnAll(boolean expected, int brokerPort, String topic, int partition,
	      String sessionId) throws Exception {
		for (Integer port : new TreeMap<>(metaServers).keySet()) {
			assertEquals(String.format("MetaServer:%d, acquire should be %s. ", port, expected), expected,
			      acquireBrokerLease(brokerPort, topic, partition, sessionId, port));
		}
	}

	private boolean acquireBrokerLease(int brokerPort, String topic, int partition, String sessionId,
	      Integer metaServerPort) throws Exception {
		LeaseAcquireResponse lease = BaseRestClient.getAcquireBrokerLease(topic, partition, sessionId, brokerPort,
		      metaServerPort);

		return lease.isAcquired();
	}

	protected void assertRenewBrokerLeaseOnAll(boolean expected, int brokerPort, String topic, int partition,
	      long leaseId, String sessionId) throws IOException, URISyntaxException {
		for (Integer port : new TreeMap<>(metaServers).keySet()) {
			assertEquals(expected, renewBrokerLease(brokerPort, topic, partition, leaseId, sessionId, port));
		}
	}

	private boolean renewBrokerLease(int brokerPort, String topic, int partition, long leaseId, String sessionId,
	      Integer metaServerPort) throws IOException, URISyntaxException {
		LeaseAcquireResponse lease = BaseRestClient.getRenewBrokerLease(topic, partition, leaseId, sessionId, brokerPort,
		      metaServerPort);
		logger.info("==Renew Lease== for Broker-Port:{}, Broker-Session:{} on Meta:{}. Got Lease:{}, isExpired:{}",
		      brokerPort, sessionId, metaServerPort, lease, lease.getLease() == null ? null : lease.getLease()
		            .isExpired());
		return lease.isAcquired();
	}

	protected void assertAcquireConsumerLeaseOnAll(boolean expected, String topic, String group) throws IOException,
	      URISyntaxException, InterruptedException {

		for (Integer port : new TreeMap<>(metaServers).keySet()) {

			// will fail at first time to acquire lease!
			acquireConsumerLease(port, topic, group);
			Thread.sleep(2000);

			assertEquals(String.format("Acquire Consumer Lease for [Topic: %s, Group: %s] on MetaServer [%d], Should be "
			      + "%s", topic, group, port, expected), expected, acquireConsumerLease(port, topic, group));
			logger.info(String.format("Acquire Consumer Lease for [Topic: %s, Group: %s] on MetaServer [%d], Is %s",
			      topic, group, port, expected));

		}
	}

	private boolean acquireConsumerLease(Integer metaServerPort, String topic, String group) throws IOException,
	      URISyntaxException {
		Tpg tpg = new Tpg();
		tpg.setTopic(topic);
		tpg.setGroupId(group);

		return BaseRestClient.getAcquireConsumerLease(metaServerPort, tpg).isAcquired();
	}

	protected void assertRenewConsumerLeaseOnAll(boolean expected, long leaseId, String topic, String group)
	      throws IOException, URISyntaxException {
		for (Integer port : new TreeMap<>(metaServers).keySet()) {
			assertEquals(expected, renewConsumerLease(port, leaseId, topic, group));
		}
	}

	private boolean renewConsumerLease(Integer metaServerPort, long leaseId, String topic, String group)
	      throws IOException, URISyntaxException {
		Tpg tpg = new Tpg();
		tpg.setTopic(topic);
		tpg.setGroupId(group);

		return BaseRestClient.getRenewConsumerLease(metaServerPort, leaseId, tpg).isAcquired();
	}

	protected List<Integer> mockBrokerRegisterToZK(int brokerCount) throws Exception {
		List<Integer> ports = new ArrayList<>();
		for (int i = 0; i < brokerCount; i++) {
			ports.add(brokerRegisterToZK());
		}
		return ports;
	}

	protected void mockBrokerAllUnRegisterToZK() throws IOException {
		for (ServiceDiscovery<Void> serviceDiscovery : tempBrokers.values()) {
			serviceDiscovery.close();
		}
		tempBrokers.clear();
	}

	// copied from DefaultBrokerRegistry
	// set session_id to the broker port!
	private int brokerRegisterToZK() throws Exception {
		int port = brokerPort.getAndIncrement();
		ServiceInstance<Void> thisInstance = ServiceInstance.<Void> builder()//
		      .name("default")//
		      .address(localhostIP)//
		      .port(port)//
		      .id(String.valueOf(port))//
		      .build();

		ServiceDiscovery<Void> m_serviceDiscovery = ServiceDiscoveryBuilder.builder(Void.class)//
		      .client(m_client.get())//
		      .basePath("brokers")//
		      .thisInstance(thisInstance)//
		      .build();
		m_serviceDiscovery.start();

		if (tempBrokers.containsKey(port)) {
			throw new RuntimeException("Already Existed in tempBrokers!!");
		} else {
			tempBrokers.put(port, m_serviceDiscovery);
			return port;
		}
	}

	private List<String> buildExpectedServers(Map<Integer, HermesClassLoaderMetaServer> metaServers) {
		List<String> result = new ArrayList<>();
		for (Integer port : metaServers.keySet()) {
			result.add(localhostIP + ":" + port);
		}
		return result;
	}

	protected void assertAllStopped() {
		assertEquals(0, metaServers.size());
	}

	protected void addOneTopicToMeta(String name) throws Exception {
		Topic topic = TopicHelper.buildTopic(name);
		tempTopics.push(topic);

		baseMetaCreateTopic(topic);
	}

	protected void addOneTopicToMeta() throws Exception {
		Topic topic = TopicHelper.buildRandomTopic();
		tempTopics.push(topic);

		baseMetaCreateTopic(topic);
	}

	protected Topic removeOneTopicFromMeta() throws Exception {
		Topic topic = null;
		if (!tempTopics.isEmpty()) {
			topic = tempTopics.pop();
			baseMetaDeleteTopic(topic);
		}
		return topic;
	}

	protected void baseMetaCreateTopic(Topic topic) throws Exception {
		Meta meta = MetaHelper.loadMeta();
		meta.addTopic(topic);

		updateVersionAndOutputAndUpdateZKVersion(meta);
	}

	protected void baseMetaUpdateTopic(Topic topic) throws Exception {
		Meta meta = MetaHelper.loadMeta();
		meta.removeTopic(topic.getName());
		topic.setLastModifiedTime(new Date(System.currentTimeMillis()));
		meta.addTopic(topic);

		updateVersionAndOutputAndUpdateZKVersion(meta);
	}

	private void updateVersionAndOutputAndUpdateZKVersion(Meta meta) throws Exception {
		meta.setVersion(meta.getVersion() + 1);

		MetaHelper.outputToTestXml(meta);
		zkService.updateZkBaseMetaVersion(zkVersion.getAndIncrement());
	}

	protected void baseMetaDeleteTopic(Topic topic) throws Exception {
		Meta meta = MetaHelper.loadMeta();
		meta.removeTopic(topic.getName());

		updateVersionAndOutputAndUpdateZKVersion(meta);
	}

	protected void waitOneLeaseTime() throws InterruptedException {
		long time = MockMetaServerConfig.BROKER_LEASE_TIMEOUT + 500;
		// Timeout time change to 3000 in MockMetaServerConfig
		logger.info("==Wait {} ms to Timeout", time);
		Thread.sleep(time);
	}

	static class TopicHelper {
		protected static Topic buildRandomTopic() {
			return buildTopic(String.valueOf(new Random().nextInt(1000)));
		}

		protected static Topic buildTopic(String name) {
			Topic topic = new Topic(name);
			topic.setAckTimeoutSeconds(555);
			topic.setCodecType("json");
			topic.setConsumerRetryPolicy("1:[3,6,9]");
			topic.setCreateTime(new Date());
			topic.setEndpointType("broker");
			topic.setId(999L);
			topic.setPartitionCount(2);
			topic.setStatus("Y");
			topic.setStorageType("mysql");

			Partition partition = new Partition();
			partition.setEndpoint("br0");
			partition.setId(1);
			partition.setReadDatasource("br0");
			partition.setWriteDatasource("br0");
			topic.addPartition(partition);
			return topic;
		}

		protected static Topic buildUpdatedTopic(Topic topic) {
			topic.setAckTimeoutSeconds(555);
			topic.setCodecType("JSON");
			topic.setConsumerRetryPolicy("1:[3,6,9]");
			topic.setCreateTime(new Date());
			topic.setEndpointType("BR0");
			topic.setId(111L);
			topic.setPartitionCount(2);
			topic.setStatus("Y");
			topic.setStorageType("MYSQL");
			return topic;
		}
	}

	static class MetaHelper {
		public static Meta loadMeta() throws FileNotFoundException {
			return loadMeta(metaXmlFile);
		}

		public static Meta loadMeta(String fileName) throws FileNotFoundException {
			InputStream in = null;
			File file;
			file = new File("src/test/resources/" + fileName);
			if (!file.exists()) {
				file = new File("hermes-metaserver/src/test/resources/" + fileName);
			}

			if (file.exists()) {
				in = new FileInputStream(file);
			}

			if (in == null) {
				logger.error("==Load Meta Error== File {} not found in classpath.", fileName);
			} else {
				try {
					return DefaultSaxParser.parse(in);
				} catch (SAXException | IOException e) {
					logger.error("==Load Meta Error== Error parse meta file {}", fileName);
				}
			}
			return null;
		}

		private static void outputToTestXml(Meta meta) {
			DefaultXmlBuilder builder = new DefaultXmlBuilder();
			try {
				PrintWriter output;
				try {
					output = new PrintWriter("src/test/resources/" + metaXmlFile);
				} catch (FileNotFoundException e) {
					output = new PrintWriter("hermes-metaserver/src/test/resources/" + metaXmlFile);
				}
				output.println(builder.buildXml(meta));
				output.flush();
			} catch (Exception ex) {
				logger.error("Error in output xml file, {}", ex);
			}
		}
	}

	static class BaseRestClient {

		public static Meta getMeta(int port) {
			WebTarget webTarget = getWebTarget(port);
			return webTarget.path("/meta").request().get().readEntity(Meta.class);
		}

		public static List<String> getMetaServers(int port) {
			WebTarget webTarget = getWebTarget(port);
			return (List<String>) webTarget.path("/metaserver/servers").request().get().readEntity(List.class);
		}

		// can't parse JSON.parseObject(json,
		// MetaServerResource.MetaStatusStatusResponse.class)
		// HermesClassLoader problem.
		// so have to parse into map/JsonObject...
		public static MetaServerStatusResponse getMetaStatusLeaderInfo(int port) throws IOException {
			WebTarget webTarget = getWebTarget(port);
			Map map = webTarget.path("/metaserver/status").request().get().readEntity(Map.class);

			MetaServerStatusResponse re = new MetaServerStatusResponse();
			re.setRole(Role.valueOf((String) map.get("role")));
			Map map2 = (Map) map.get("leaderInfo");
			HostPort hostport = new HostPort((String) map2.get("host"), (Integer) map2.get("port"));
			re.setLeaderInfo(hostport);

			return re;
		}

		public static MetaServerStatusResponse getMetaStatusBrokerAssignment(int port) throws IOException {
			WebTarget webTarget = getWebTarget(port);
			Map map = webTarget.path("/metaserver/status").request().get().readEntity(Map.class);

			MetaServerStatusResponse re = new MetaServerStatusResponse();
			Map<String, Assignment<Integer>> assignment = JSON.parseObject(
			      JSON.toJSONString(map.get("brokerAssignments")), new TypeReference<Map<String, Assignment<Integer>>>() {
			      }.getType());

			re.setRole(Role.valueOf((String) map.get("role")));
			re.setBrokerAssignments(assignment);
			return re;
		}

		public static MetaServerStatusResponse getMetaStatusMetaServerAssignment(int port) throws IOException {
			WebTarget webTarget = getWebTarget(port);
			Map map = webTarget.path("/metaserver/status").request().get().readEntity(Map.class);

			MetaServerStatusResponse re = new MetaServerStatusResponse();

			Assignment<String> assignment = JSON.parseObject(JSON.toJSONString(map.get("metaServerAssignments")),
			      new TypeReference<Assignment<String>>() {
			      }.getType());
			re.setMetaServerAssignments(assignment);
			return re;
		}

		public static LeaseAcquireResponse getAcquireBrokerLease(String topic, int partition, String sessionId,
		      int brokerPort, int metaServerPort) throws IOException, URISyntaxException {
			return getBrokerLease("/lease/broker/acquire", topic, partition, null, sessionId, brokerPort, metaServerPort);
		}

		public static LeaseAcquireResponse getRenewBrokerLease(String topic, int partition, long leaseId,
		      String sessionId, int brokerPort, int metaServerPort) throws IOException, URISyntaxException {
			return getBrokerLease("/lease/broker/renew", topic, partition, leaseId, sessionId, brokerPort, metaServerPort);
		}

		public static LeaseAcquireResponse getAcquireConsumerLease(int metaServerPort, Tpg tpg) throws IOException,
		      URISyntaxException {
			return getConsumerLease("/lease/consumer/acquire", "WRONG SESSION", null, tpg, metaServerPort);
		}

		public static LeaseAcquireResponse getRenewConsumerLease(int metaServerPort, long leaseId, Tpg tpg)
		      throws IOException, URISyntaxException {
			return getConsumerLease("/lease/consumer/renew", "WRONG SESSION", leaseId, tpg, metaServerPort);

		}

		static ObjectMapper om = new ObjectMapper();

		public static LeaseAcquireResponse getConsumerLease(String uri, String sessionId, Long leaseId, Tpg tpg,
		      int metaServerPort) throws URISyntaxException, IOException {
			URIBuilder uriBuilder = new URIBuilder()//
			      .setScheme("http")//
			      .setHost(localhostIP)//
			      .setPort(metaServerPort)//
			      .setPath(uri);

			Map<String, String> params = new HashMap<>();
			params.put("sessionId", sessionId);
			params.put("host", localhostIP);

			if (null != leaseId) {
				params.put("leaseId", Long.toString(leaseId));
			}
			for (Map.Entry<String, String> entry : params.entrySet()) {
				uriBuilder.addParameter(entry.getKey(), entry.getValue());
			}

			return getLeaseAcquireResponse(uriBuilder, tpg);
		}

		public static LeaseAcquireResponse getBrokerLease(String uri, String topic, int partition, Long leaseId,
		      String sessionId, int brokerPort, int metaServerPort) throws URISyntaxException, IOException {
			URIBuilder uriBuilder = new URIBuilder()//
			      .setScheme("http")//
			      .setHost(localhostIP)//
			      .setPort(metaServerPort)//
			      .setPath(uri);

			Map<String, String> params = new HashMap<>();
			params.put("topic", topic);
			params.put("partition", Integer.toString(partition));
			params.put("sessionId", sessionId);
			params.put("brokerPort", Integer.toString(brokerPort));
			params.put("host", localhostIP);

			if (null != leaseId) {
				params.put("leaseId", Long.toString(leaseId));
			}
			for (Map.Entry<String, String> entry : params.entrySet()) {
				uriBuilder.addParameter(entry.getKey(), entry.getValue());
			}

			return getLeaseAcquireResponse(uriBuilder, null);
		}

		private static LeaseAcquireResponse getLeaseAcquireResponse(URIBuilder uriBuilder, Object payload)
		      throws IOException, URISyntaxException {
			HttpResponse response;
			if (payload != null) {
				response = Request.Post(uriBuilder.build())//
				      .bodyString(JSON.toJSONString(payload), ContentType.APPLICATION_JSON)//
				      .execute()//
				      .returnResponse();
			} else {
				response = Request.Post(uriBuilder.build())//
				      .execute()//
				      .returnResponse();
			}

			if (response != null && response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
				String responseContent = EntityUtils.toString(response.getEntity());
				if (!StringUtils.isBlank(responseContent)) {

					/**
					 * trust me, you can't use: JSON.parseObject(response, LeaseAcquireResponse.class); you'll get Exception:
					 * "set property error, lease", "object is not an instance of declaring class"
					 *
					 * That's maybe caused by that Lease is loaded by HermesClassLoader, same to the problem in
					 * MetaStatusStatusResponse.Assignment<String>.
					 *
					 * Unable to solve this. As we need HermesClassLoader to launch different Plexus, so that we can run several
					 * metaservers at same time.
					 */
					return om.readValue(responseContent, LeaseAcquireResponse.class);
				}
			}
			return null;
		}

		private static WebTarget getWebTarget(int port) {
			Client client = ClientBuilder.newClient();
			return client.target("http://127.0.0.1:" + port);
		}
	}
}
